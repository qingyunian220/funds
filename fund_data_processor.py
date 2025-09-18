import requests
import json
import re
import pandas as pd
from typing import Dict, List, Optional, Tuple
import akshare as ak
from datetime import datetime, timedelta
import os

from fund_search_parser import fetch_and_parse_fund_search


def get_fund_name_by_code(fund_code: str) -> Optional[str]:
    """
    根据基金代码查找基金名称
    
    Args:
        fund_code (str): 基金代码
        
    Returns:
        str: 基金名称，如果获取失败则返回None
    """
    try:
        # # 使用akshare获取基金基本信息
        # fund_info = ak.fund_individual_basic_info_xq(symbol=fund_code)
        # # 提取基金名称
        # fund_name_row = fund_info[fund_info['item'] == '基金名称']
        # if not fund_name_row.empty:
        #     return fund_name_row['value'].iloc[0]
        # 读取jiuquaner_with_names.xlsx文件
        fund_list = pd.read_excel('fund_list.xlsx')
        # 确保基金代码列是字符串类型，并且是6位数格式
        fund_list['基金代码'] = fund_list['基金代码'].astype(str).str.zfill(6)
        # 通过fund_list中的code列匹配fund_code 得到fund_name
        a = fund_list[fund_list['基金代码'] == fund_code]
        fund_name = fund_list[fund_list['基金代码'] == fund_code]['基金简称']
        # 只获取基金名称字符串
        if not fund_name.empty:
            fund_name_str = fund_name.iloc[0]
            return fund_name_str
        else:
            print("未找到匹配的基金")
    except Exception as e:
        print(f"获取基金 {fund_code} 名称时出错: {e}")
    return None


def find_a_c_classes(fund_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    根据基金名称查找对应的A类和C类基金名称
    
    Args:
        fund_name (str): 基金名称
        
    Returns:
        tuple: (A类基金名称, C类基金名称)，如果未找到则对应位置为None
    """
    # 移除可能的A类或C类后缀
    base_name = fund_name
    if fund_name.endswith('A') or fund_name.endswith('C'):
        base_name = fund_name[:-1]
    
    # 构造A类和C类基金名称
    a_class_name = base_name + 'A' if not base_name.endswith('A') else base_name
    c_class_name = base_name + 'C' if not base_name.endswith('C') else base_name
    
    # 如果原始名称就是A类或C类，则直接返回
    if fund_name.endswith('A'):
        return fund_name, c_class_name
    elif fund_name.endswith('C'):
        return a_class_name, fund_name
    else:
        # 对于没有明确后缀的基金，返回构造的A类和C类名称
        return a_class_name, c_class_name


def search_fund_code_by_name(fund_name: str) -> Optional[str]:
    """
    根据基金名称查找对应的基金代码

    Args:
        fund_name (str): 基金名称

    Returns:
        str: 基金代码，如果未找到则返回None
    """
    try:
        # 使用akshare搜索基金
        search_result = ak.fund_name_search_em(fund_name)
        if not search_result.empty:
            # 查找完全匹配的基金名称
            exact_match = search_result[search_result['基金名称'] == fund_name]
            if not exact_match.empty:
                return exact_match['基金代码'].iloc[0]
    except Exception as e:
        print(f"搜索基金 {fund_name} 代码时出错: {e}")

    return None


def crawl_fund_scale_data(fund_code: str) -> Dict:
    """
    爬取基金规模数据
    
    Args:
        fund_code (str): 基金代码
        
    Returns:
        dict: 包含基金规模数据的字典
    """
    # 基金规模数据API
    url = f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    
    params = {
        "type": "gmbd",
        "mode": "0",
        "code": fund_code,
        "rt": "0.3293533905262933"
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Referer': f'http://fundf10.eastmoney.com/gmbd_{fund_code}.html'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.encoding = 'utf-8'
        
        # 解析返回的数据
        text = response.text
        
        # 使用正则表达式提取data字段中的JSON数据
        data_match = re.search(r'"data":\s*(\[[^\]]*\])', text)
        if not data_match:
            return {
                'fund_code': fund_code,
                'data': None,
                'status': 'failed',
                'message': '未能找到份额/净资产规模变动详情数据'
            }
        
        # 解析JSON数据
        data_str = data_match.group(1)
        data = json.loads(data_str)
        
        if data:
            # 转换数据格式以便于处理，只保留日期和期末净资产两个字段
            processed_data = [{
                '日期': item.get('FSRQ', ''),
                '期末净资产': round(item.get('NETNAV', 0) / 100000000, 2) if item.get('NETNAV') else None
            } for item in data]
            
            # 按日期排序并只保留前5条数据
            processed_data.sort(key=lambda x: x['日期'], reverse=True)
            processed_data = processed_data[:5]
            
            return {
                'fund_code': fund_code,
                'data': processed_data,
                'status': 'success'
            }
        
        return {
            'fund_code': fund_code,
            'data': None,
            'status': 'failed',
            'message': '未能找到份额/净资产规模变动详情数据'
        }
        
    except Exception as e:
        return {
            'fund_code': fund_code,
            'data': None,
            'status': 'error',
            'message': f'爬取数据时发生错误: {str(e)}'
        }


def crawl_fund_cyrjg_data(fund_code: str) -> Dict:
    """
    爬取基金持有人结构数据

    Args:
        fund_code (str): 基金代码

    Returns:
        dict: 包含基金持有人结构数据的字典
    """
    # 基金持有人结构数据API
    url = f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx"

    params = {
        "type": "cyrjg",
        "code": fund_code,
        "rt": "0.3293533905262933"
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Referer': f'http://fundf10.eastmoney.com/cyrjg_{fund_code}.html'
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.encoding = 'utf-8'

        # 解析返回的数据
        text = response.text
        # print("持有人结构数据:",text)

        # 使用正则表达式提取apidata.content中的HTML表格数据
        apidata_match = re.search(r'var apidata=\{ content:"([^"]*)"', text)
        if not apidata_match:
            return {
                'fund_code': fund_code,
                'data': None,
                'status': 'failed',
                'message': '未能找到持有人结构数据'
            }
        
        # 获取HTML内容
        html_content = apidata_match.group(1)
        
        # 使用正则表达式提取表格中的数据行
        # 提取表格行，排除表头
        row_pattern = r'<tr><td>(\d{4}-\d{2}-\d{2})</td><td class=\'tor\'>([^<]*)</td><td class=\'tor\'>([^<]*)</td><td class=\'tor\'>([^<]*)</td><td class=\'tor\'>([^<]*)</td></tr>'
        rows = re.findall(row_pattern, html_content)
        
        if not rows:
            return {
                'fund_code': fund_code,
                'data': None,
                'status': 'failed',
                'message': '未能解析持有人结构数据'
            }
        
        # 处理数据，提取前5条
        processed_data = []
        for row in rows[:5]:  # 只取前5条
            date, institution_ratio, individual_ratio, internal_ratio, total_shares = row
            processed_data.append({
                '日期': date,
                '机构持有比例': institution_ratio,
                # '个人持有比例': individual_ratio,
                # '内部持有比例': internal_ratio,
                '总份额（亿份）': total_shares
            })
        
        return {
            'fund_code': fund_code,
            'data': processed_data,
            'status': 'success'
        }

    except Exception as e:
        return {
            'fund_code': fund_code,
            'data': None,
            'status': 'error',
            'message': f'爬取数据时发生错误: {str(e)}'
        }


def aggregate_fund_scale_data(scale_data_list: List[Dict]) -> List[Dict]:
    """
    聚合基金规模数据
    
    Args:
        scale_data_list (list): 基金规模数据列表
        
    Returns:
        list: 聚合后的基金规模数据
    """
    if not scale_data_list:
        return []
    
    # 按日期分组并聚合规模数据
    date_scale_map = {}
    
    for scale_data in scale_data_list:
        if scale_data.get('status') != 'success' or not scale_data.get('data'):
            continue
            
        for item in scale_data['data']:
            date = item['日期']
            net_asset = item['期末净资产'] or 0
            # 保留两位小数
            net_asset = round(float(net_asset), 2)
            
            if date not in date_scale_map:
                date_scale_map[date] = {
                    '日期': date,
                    '期末净资产': 0,
                    '基金明细': []
                }
            
            date_scale_map[date]['期末净资产'] += net_asset
            # 保留两位小数
            date_scale_map[date]['期末净资产'] = round(date_scale_map[date]['期末净资产'], 2)
            date_scale_map[date]['基金明细'].append({
                '基金代码': scale_data['fund_code'],
                '基金名称': get_fund_name_by_code(scale_data['fund_code']) or '未知',
                '期末净资产': net_asset
            })
    
    # 转换为列表并按日期排序
    result = list(date_scale_map.values())
    result.sort(key=lambda x: x['日期'], reverse=True)
    
    # 只保留前5条记录
    return result[:5]


def aggregate_fund_cyrjg_data(cyrjg_data_list: List[Dict]) -> List[Dict]:
    """
    聚合基金持有人结构数据
    
    Args:
        cyrjg_data_list (list): 基金持有人结构数据列表
        
    Returns:
        list: 聚合后的基金持有人结构数据
    """
    if not cyrjg_data_list:
        return []
    
    # 按日期分组并聚合持有人结构数据
    date_cyrjg_map = {}
    
    for cyrjg_data in cyrjg_data_list:
        if cyrjg_data.get('status') != 'success' or not cyrjg_data.get('data'):
            continue
            
        for item in cyrjg_data['data']:
            date = item['日期']
            # 解析数据，将字符串转换为数值
            try:
                institution_ratio = float(item['机构持有比例'].rstrip('%')) if item['机构持有比例'].endswith('%') else float(item['机构持有比例'])
            except ValueError:
                # 处理无法转换的情况（如'---'），将其视为0
                institution_ratio = 0
            
            try:
                total_shares = float(item['总份额（亿份）']) if item['总份额（亿份）'] != '' else 0
            except ValueError:
                # 处理无法转换的情况，将其视为0
                total_shares = 0
            
            if date not in date_cyrjg_map:
                date_cyrjg_map[date] = {
                    '日期': date,
                    '机构持有比例': 0,
                    # '个人持有比例': 0,
                    # '内部持有比例': 0,
                    '总份额（亿份）': 0,
                    '基金明细': []
                }
            
            # 使用加权平均方法计算机构持有比例
            # 加权总份额 = 之前总份额 + 当前基金总份额
            weighted_shares_sum = date_cyrjg_map[date]['总份额（亿份）'] + total_shares
            
            if weighted_shares_sum > 0:
                # 加权机构持有比例 = (之前机构持有比例*之前总份额 + 当前机构持有比例*当前总份额) / 加权总份额
                weighted_institution_ratio = (
                    date_cyrjg_map[date]['机构持有比例'] * date_cyrjg_map[date]['总份额（亿份）'] +
                    institution_ratio * total_shares
                ) / weighted_shares_sum
                
                # 更新聚合数据，保留两位小数
                date_cyrjg_map[date]['机构持有比例'] = round(weighted_institution_ratio, 2)
                date_cyrjg_map[date]['总份额（亿份）'] = round(weighted_shares_sum, 2)
            else:
                # 如果总份额为0，则直接使用当前值，保留两位小数
                date_cyrjg_map[date]['机构持有比例'] = round(institution_ratio, 2)
                date_cyrjg_map[date]['总份额（亿份）'] = round(total_shares, 2)
            
            date_cyrjg_map[date]['基金明细'].append({
                '基金代码': cyrjg_data['fund_code'],
                '基金名称': get_fund_name_by_code(cyrjg_data['fund_code']) or '未知',
                '机构持有比例': item['机构持有比例'],
                # '个人持有比例': item['个人持有比例'],
                # '内部持有比例': item['内部持有比例'],
                '总份额（亿份）': item['总份额（亿份）']
            })
    
    # 格式化机构持有比例为百分比字符串，保留两位小数
    for date_data in date_cyrjg_map.values():
        date_data['机构持有比例'] = f"{date_data['机构持有比例']:.2f}%"
    
    # 转换为列表并按日期排序
    result = list(date_cyrjg_map.values())
    result.sort(key=lambda x: x['日期'], reverse=True)
    
    # 只保留前5条记录
    return result[:5]


def update_fund_data_json(target_fund_code: str, target_fund_name: str, 
                         aggregated_scale_data: List[Dict], aggregated_cyrjg_data: List[Dict]):
    """
    更新基金数据到JSON文件
    
    Args:
        target_fund_code (str): 目标基金代码
        target_fund_name (str): 目标基金名称
        aggregated_scale_data (list): 聚合后的基金规模数据
        aggregated_cyrjg_data (list): 聚合后的基金持有人结构数据
    """
    fund_data_file = 'fund_data.json'
    
    # 读取现有数据
    if os.path.exists(fund_data_file):
        with open(fund_data_file, 'r', encoding='utf-8') as f:
            fund_data = json.load(f)
    else:
        fund_data = {}
    
    # 更新基金数据
    if target_fund_code not in fund_data:
        fund_data[target_fund_code] = {}
        
    fund_data[target_fund_code].update({
        '基金名称': target_fund_name,
        '规模数据': aggregated_scale_data,
        '持有人结构': aggregated_cyrjg_data,
        '更新时间': datetime.now().strftime('%Y-%m-%d')  # 添加更新时间
    })
    
    # 写入文件
    with open(fund_data_file, 'w', encoding='utf-8') as f:
        json.dump(fund_data, f, ensure_ascii=False, indent=2)
    
    print(f"已更新基金 {target_fund_code} 的数据到 {fund_data_file}")


def process_fund_data(original_fund_code: str):
    """
    处理基金数据的完整流程
    
    Args:
        original_fund_code (str): 原始基金代码
    """
    print(f"开始处理基金代码: {original_fund_code}")
    
    # 1. 根据基金代码查找基金名称
    fund_name = get_fund_name_by_code(original_fund_code)
    if not fund_name:
        print(f"无法获取基金 {original_fund_code} 的名称")
        return
    
    print(f"基金名称: {fund_name}")
    base_name = fund_name
    if fund_name.endswith('A') or fund_name.endswith('C'):
        base_name = fund_name[:-1]
    # 2. 根据基金名称查找对应的基金代码(A+C)
    # [{'code': '015381', 'name': '东方兴瑞趋势领航混合A'}, {'code': '015382', 'name': '东方兴瑞趋势领航混合C'}]
    code_names = fetch_and_parse_fund_search(base_name)
    # 3. 获取A类和C类基金的规模数据
    scale_data_list = []
    cyrjg_data_list = []
    for code_name in code_names:
        scale_data = crawl_fund_scale_data(code_name['code'])
        scale_data_list.append(scale_data)
        cyrjg_data = crawl_fund_cyrjg_data(code_name['code'])
        cyrjg_data_list.append(cyrjg_data)
    # 4. 聚合规模数据
    print("正在聚合基金规模数据...")
    aggregated_scale_data = aggregate_fund_scale_data(scale_data_list)
    
    if not aggregated_scale_data:
        print("未能获取有效的基金规模数据")
        return
    # 显示聚合后的数据
    print("聚合后的基金规模数据:")
    for item in aggregated_scale_data:
        print(f"  日期: {item['日期']}, 期末净资产: {item['期末净资产']} 亿元")
    # 5. 聚合持有人结构数据
    print("正在聚合基金持有人结构数据...")
    aggregated_cyrjg_data = aggregate_fund_cyrjg_data(cyrjg_data_list)
    print("聚合后的基金持有人结构数据:")
    for item in aggregated_cyrjg_data:
        print(f"  日期: {item['日期']},机构持有比例:{item['机构持有比例']}")
        # for detail in item['基金明细']:
        #     print(f"    - {detail['基金代码']} {detail['基金名称']}: {detail['机构持有比例']}")
    # 6. 更新到fund_data.json中
    target_fund_code = original_fund_code
    target_fund_name = fund_name
    update_fund_data_json(target_fund_code, target_fund_name, aggregated_scale_data, aggregated_cyrjg_data)


def process_fund_data_with_cache(original_fund_code: str, cache_days: int = 90):
    """
    处理基金数据的完整流程（带缓存机制）
    
    Args:
        original_fund_code (str): 原始基金代码
        cache_days (int): 缓存天数，默认90天（约一个季度）
    """
    print(f"开始处理基金代码: {original_fund_code}")
    
    # 检查是否有缓存数据且未过期
    fund_data_file = 'fund_data.json'
    if os.path.exists(fund_data_file):
        try:
            with open(fund_data_file, 'r', encoding='utf-8') as f:
                all_fund_data = json.load(f)
                
            if original_fund_code in all_fund_data:
                fund_info = all_fund_data[original_fund_code]
                # 检查更新时间
                if '更新时间' in fund_info:
                    update_time_str = fund_info['更新时间']
                    update_time = datetime.strptime(update_time_str, '%Y-%m-%d')
                    # 如果距离现在不到cache_days天，则使用缓存数据
                    if datetime.now() - update_time < timedelta(days=cache_days):
                        print(f"基金 {original_fund_code} 的数据在 {cache_days} 天内已更新，使用缓存数据")
                        return
        except Exception as e:
            print(f"读取缓存数据时出错: {e}")
    
    # 如果没有缓存或者缓存已过期，则执行完整流程
    print(f"基金 {original_fund_code} 需要更新数据...")
    
    # 1. 根据基金代码查找基金名称
    fund_name = get_fund_name_by_code(original_fund_code)
    if not fund_name:
        print(f"无法获取基金 {original_fund_code} 的名称")
        return
    
    print(f"基金名称: {fund_name}")
    base_name = fund_name
    if fund_name.endswith('A') or fund_name.endswith('C'):
        base_name = fund_name[:-1]
    # 2. 根据基金名称查找对应的基金代码(A+C)
    # [{'code': '015381', 'name': '东方兴瑞趋势领航混合A'}, {'code': '015382', 'name': '东方兴瑞趋势领航混合C'}]
    code_names = fetch_and_parse_fund_search(base_name)
    # 3. 获取A类和C类基金的规模数据
    scale_data_list = []
    cyrjg_data_list = []
    for code_name in code_names:
        scale_data = crawl_fund_scale_data(code_name['code'])
        scale_data_list.append(scale_data)
        cyrjg_data = crawl_fund_cyrjg_data(code_name['code'])
        cyrjg_data_list.append(cyrjg_data)
    # 4. 聚合规模数据
    print("正在聚合基金规模数据...")
    aggregated_scale_data = aggregate_fund_scale_data(scale_data_list)
    
    if not aggregated_scale_data:
        print("未能获取有效的基金规模数据")
        return
    # 显示聚合后的数据
    print("聚合后的基金规模数据:")
    for item in aggregated_scale_data:
        print(f"  日期: {item['日期']}, 期末净资产: {item['期末净资产']} 亿元")
    # 5. 聚合持有人结构数据
    print("正在聚合基金持有人结构数据...")
    aggregated_cyrjg_data = aggregate_fund_cyrjg_data(cyrjg_data_list)
    print("聚合后的基金持有人结构数据:")
    for item in aggregated_cyrjg_data:
        print(f"  日期: {item['日期']},机构持有比例:{item['机构持有比例']}")
        # for detail in item['基金明细']:
        #     print(f"    - {detail['基金代码']} {detail['基金名称']}: {detail['机构持有比例']}")
    # 6. 更新到fund_data.json中
    target_fund_code = original_fund_code
    target_fund_name = fund_name
    update_fund_data_json(target_fund_code, target_fund_name, aggregated_scale_data, aggregated_cyrjg_data)


if __name__ == "__main__":
    # 示例使用
    fund_code = '013642'
    # print(crawl_fund_cyrjg_data(fund_code))
    print(process_fund_data(fund_code))
