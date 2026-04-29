import requests
import pandas as pd
import json
import time
import gzip
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

HEADER_JIUQUAN = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Content-Type': 'application/json',
    'Referer': 'https://www.funddb.cn/',
    'Origin': 'https://www.funddb.cn'
}

def create_session():
    session = requests.Session()
    session.headers.update(HEADER_JIUQUAN)
    return session

def decompress_response_content(response):
    content = response.content
    try:
        if response.headers.get('Content-Encoding') == 'gzip':
            content = gzip.decompress(content)
    except:
        pass
    return content

def get_fund_style_data(fund_code):
    """
    获取基金持股风格数据
    """
    url = "https://api.jiucaishuo.com/fundetail/fund-position/fundinvest"
    payload = {
        "fund_code": fund_code,
        "tp": "cg",
        "category": "zz_category"
    }

    # 每次创建新的session（线程安全）
    session = requests.Session()
    session.headers.update(HEADER_JIUQUAN)
    
    try:
        response = session.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        content = decompress_response_content(response)
        
        try:
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                text = content.decode('gbk')
            except UnicodeDecodeError:
                text = content.decode('utf-8', errors='ignore')
        
        text = text.strip()
        
        if not text:
            return None
            
        if not (text.startswith('{') or text.startswith('[')):
            return None
            
        data = json.loads(text)
        
        if data['code'] != 0:
            return None

        infos = data.get('data', {}).get('cg', {}).get('infos', [])
        if infos:
            style_data = {'基金代码': fund_code}
            for info in infos:
                name = info.get('name', '')
                num1 = info.get('num1', '')
                num2 = info.get('num2', '')
                style_data[name] = f"{num1}/{num2}"
                style_data[f"{name}_本基金"] = num1
                style_data[f"{name}_同类平均"] = num2
            return style_data
        return None

    except Exception:
        return None

def get_fund_list(filter_types=None):
    """获取天天基金的基金列表
    filter_types: 可选，只保留指定类型的基金，如 ['股票', '混合', '指数']
    """
    url = 'http://fund.eastmoney.com/js/fundcode_search.js'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    response = requests.get(url, headers=headers, timeout=10)
    content = response.text
    data_str = re.findall(r'var r = (\[.+\]);', content, re.S)[0]
    fund_list = json.loads(data_str)
    
    # 先查看数据结构
    if fund_list:
        sample = fund_list[0]
        print(f'基金列表数据结构: 每只基金有 {len(sample)} 个字段')
        print(f'示例数据: {sample}\n')
    
    df = pd.DataFrame(fund_list)
    
    # 设置列名 - 根据数据结构: [代码, 缩写, 名称, 类型, 拼音]
    if len(df.columns) == 5:
        df.columns = ['code', 'abbr', 'name', 'type', 'pinyin']
    elif len(df.columns) == 4:
        df.columns = ['code', 'abbr', 'name', 'type']
    elif len(df.columns) >= 3:
        df.columns = ['code', 'name', 'type'] + [f'col_{i}' for i in range(3, len(df.columns))]
    
    # 打印所有基金类型
    if 'type' in df.columns:
        print(f'基金类型示例: {df["type"].unique()[:10]}\n')
        
        # 筛选指定类型 - 使用关键词匹配（因为类型是"混合型-灵活"这种格式）
        if filter_types:
            print(f'筛选基金类型关键词: {filter_types}')
            mask = df['type'].str.contains('|'.join(filter_types), na=False)
            df = df[mask].copy()
            print(f'筛选后剩余 {len(df)} 只基金\n')
    
    return df

def batch_get_style(fund_codes, fund_name_map=None, max_workers=10, filter_market_cap_threshold=None):
    """批量获取基金风格评分（多线程版）
    fund_name_map: 基金代码->名称的字典
    max_workers: 并发线程数，默认10
    """
    results = []
    filtered_results = []
    total = len(fund_codes)
    lock = Lock()
    success_count = 0
    
    print(f'开始多线程获取，并发数: {max_workers}\n')
    
    def fetch_fund(code):
        nonlocal success_count
        try:
            style = get_fund_style_data(code)
            if style:
                # 添加基金名称
                if fund_name_map and code in fund_name_map:
                    style['基金名称'] = fund_name_map[code]
                
                # 检查市值是否为0，为0则跳过
                market_cap_score = style.get('市值_本基金')
                if market_cap_score is not None and str(market_cap_score).strip() in ['0', '0.0', '0.00']:
                    return
                
                with lock:
                    results.append(style)
                    success_count += 1
                    
                    # 如果设置了市值筛选阈值
                    if filter_market_cap_threshold is not None:
                        if market_cap_score and float(market_cap_score) < filter_market_cap_threshold:
                            filtered_results.append(style)
                            fund_name = style.get('基金名称', code)
                            print(f'  ✓ [{success_count}/{total}] {fund_name}({code}) - 市值评分 {market_cap_score} < {filter_market_cap_threshold}，已筛选')
                    else:
                        if success_count % 50 == 0 or success_count == total:
                            print(f'  [{success_count}/{total}] 已获取 {success_count} 只基金')
        except Exception as e:
            pass
    
    # 使用线程池
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_fund, code) for code in fund_codes]
        
        for future in as_completed(futures):
            future.result()  # 获取结果，确保异常被抛出
    
    print(f'\n完成！共获取 {len(results)} 只基金数据（已过滤市值为0的）\n')
    
    df_all = pd.DataFrame(results)
    
    # 调整列顺序，把基金名称放在第二列
    if not df_all.empty and '基金名称' in df_all.columns:
        cols = ['基金代码', '基金名称'] + [col for col in df_all.columns if col not in ['基金代码', '基金名称']]
        df_all = df_all[cols]
    
    if filter_market_cap_threshold is not None:
        df_filtered = pd.DataFrame(filtered_results)
        if not df_filtered.empty and '基金名称' in df_filtered.columns:
            cols = ['基金代码', '基金名称'] + [col for col in df_filtered.columns if col not in ['基金代码', '基金名称']]
            df_filtered = df_filtered[cols]
        return df_all, df_filtered
    else:
        return df_all, None

def main():
    print('='*60)
    print('韭圈儿基金风格评分批量获取工具')
    print('='*60)
    
    # 直接使用默认配置
    filter_threshold = 25
    max_workers = 20
    
    print('\n配置:')
    print('  ✓ 扫描全部: 股票型、混合型、指数型基金')
    print('  ✓ 排除债券、定开、货币基金')
    print('  ✓ 筛选条件: 市值评分 < 25')
    print('  ✓ 并发线程: 20')
    print('\n正在获取基金列表...')
    
    fund_list_df = get_fund_list(filter_types=['股票', '混合', '指数'])
    
    # 提前过滤债券和定开基金
    print('\n提前过滤债券、定开、货币基金...')
    if 'name' in fund_list_df.columns:
        mask = ~fund_list_df['name'].str.contains('债券|定开|货币|人民币|美元|300|500|定期|纯债|A50|持有|黄金|A', na=False)
        removed = fund_list_df[~mask]
        if len(removed) > 0:
            print(f'  - 排除含有"债券"或"定开"或"货币"的基金: {len(removed)} 只')
            for name in removed['name'].head(10):
                print(f'    - {name}')
            if len(removed) > 10:
                print(f'    ... 还有 {len(removed)-10} 只')
        fund_list_df = fund_list_df[mask].copy()
        print(f'  - 剩余待扫描基金: {len(fund_list_df)} 只\n')
    
    # 保存待扫描基金列表
    # scan_list_filename = f'fund_scan_list_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    # fund_list_df.to_csv(scan_list_filename, index=False, encoding='utf-8-sig')
    # print(f'待扫描基金列表已保存到: {scan_list_filename}\n')
    
    fund_codes = fund_list_df['code'].tolist()
    
    # 创建基金代码->名称的映射
    fund_name_map = None
    if 'code' in fund_list_df.columns and 'name' in fund_list_df.columns:
        fund_name_map = dict(zip(fund_list_df['code'], fund_list_df['name']))
        print(f'已加载 {len(fund_name_map)} 只基金的名称')
    
    print(f'\n开始多线程获取 {len(fund_codes)} 只基金的风格评分...\n')
    result_df, filtered_df = batch_get_style(fund_codes, fund_name_map=fund_name_map, max_workers=max_workers, filter_market_cap_threshold=filter_threshold)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 处理A/C类基金：如果同时有A类和C类，只保留C类
    def filter_ac_funds(df):
        if df.empty:
            return df
        
        df = df.copy()
        
        # 创建基金基础名称（去掉A/C后缀）
        def get_base_name(name):
            if not isinstance(name, str):
                return str(name)
            # 去掉常见后缀
            name = name.strip()
            for suffix in ['A', 'C', 'B', 'E', 'D', 'F']:
                if name.endswith(suffix):
                    return name[:-1].rstrip(' ABCDEF')
            # 检查中间是否有A/C（如"XX混合A类"）
            import re
            match = re.search(r'(.*?)(?:A|C|A类|C类)(?:$|\s|$)', name)
            if match:
                return match.group(1).rstrip()
            return name
        
        df['基础名称'] = df['基金名称'].apply(get_base_name)
        
        # 按基础名称分组
        base_groups = df.groupby('基础名称')
        
        keep_indices = []
        
        for base_name, group in base_groups:
            # 检查是否有C类
            c_mask = group['基金名称'].str.contains(r'C$|C类| C', na=False)
            has_c = c_mask.any()
            
            if has_c:
                # 只保留C类
                c_indices = group[c_mask].index
                keep_indices.extend(c_indices)
                if len(group) > len(c_indices):
                    a_names = group[~c_mask]['基金名称'].tolist()
                    print(f'  - 同时有A/C类，保留C类，移除: {a_names}')
            else:
                # 没有C类，保留全部
                keep_indices.extend(group.index)
        
        # 过滤并删除临时列
        result_df = df.loc[keep_indices].copy()
        result_df = result_df.drop('基础名称', axis=1)
        
        return result_df
    
    # 处理A/C类并保存
    print('\n处理A/C类基金...')
    if filtered_df is not None and len(filtered_df) > 0:
        filtered_df = filter_ac_funds(filtered_df)
    
    # 保存全部数据
    filename_all = f'fund_style_all_{timestamp}.csv'
    result_df.to_csv(filename_all, index=False, encoding='utf-8-sig')
    print(f'\n全部数据已保存到: {filename_all}')
    
    print('\n' + '='*60)
    print('全部数据预览:')
    print('='*60)
    print(result_df.to_string())
    
    # 处理并保存筛选结果（如果有）
    if filter_threshold is not None and filtered_df is not None and len(filtered_df) > 0:
        print('\n' + '='*60)
        print(f'✓ 找到 {len(filtered_df)} 只市值评分 < {filter_threshold} 的基金（已处理A/C类）:')
        print('='*60)
        print(filtered_df.to_string())
        
        filename_filtered = f'fund_style_market_cap_lt{filter_threshold}_{timestamp}.csv'
        filtered_df.to_csv(filename_filtered, index=False, encoding='utf-8-sig')
        print(f'\n筛选结果已保存到: {filename_filtered}')
    elif filter_threshold is not None:
        print(f'\n未找到市值评分 < {filter_threshold} 的基金')

if __name__ == '__main__':
    main()
