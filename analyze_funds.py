import re

import pandas as pd
import akshare as ak
from datetime import datetime
from tqdm import tqdm
from enhanced_index import get_fund_info
from fund_data_processor import get_fund_name_by_code
from fund_search_parser import fetch_and_parse_fund_search
from jiuquan_fund import parse_fund_data

def get_top10_stocks_weight_robust(fund_code):
    """
    获取指定基金最近季度前十大重仓股的占净值比例之和
    使用多种方法处理编码问题
    """
    current_year = str(datetime.now().year)
    previous_year = str(datetime.now().year - 1)

    # 尝试不同方法获取数据
    fund_portfolio_hold_em_df = None

    # 方法1: 直接使用akshare，处理可能的编码错误
    try:
        fund_portfolio_hold_em_df = ak.fund_portfolio_hold_em(symbol=fund_code, date=current_year)
    except KeyError as e:
        print(f"当前年份 {current_year} 数据获取失败: {e}")
        try:
            fund_portfolio_hold_em_df = ak.fund_portfolio_hold_em(symbol=fund_code, date=previous_year)
        except KeyError as e2:
            print(f"上一年份 {previous_year} 数据获取也失败: {e2}")
            return None

    if fund_portfolio_hold_em_df is None or fund_portfolio_hold_em_df.empty:
        print(f"{current_year}年和{previous_year}年数据都为空")
        return None

    print("原始数据:")
    print(fund_portfolio_hold_em_df)

    # 获取所有不同的季度
    try:
        quarters = fund_portfolio_hold_em_df['季度'].unique()
    except KeyError:
        # 如果季度列名有问题，尝试其他可能的列名
        possible_date_cols = [col for col in fund_portfolio_hold_em_df.columns if '季度' in col or 'date' in col.lower() or 'period' in col.lower()]
        if possible_date_cols:
            quarters = fund_portfolio_hold_em_df[possible_date_cols[0]].unique()
        else:
            print("找不到季度列")
            return None

    print(f"\n所有季度: {quarters}")

    # 确定最新的季度
    latest_quarter = sorted(quarters, reverse=True)[0]
    print(f"\n最新季度: {latest_quarter}")

    # 提取最近一个季度的数据
    try:
        filtered_df = fund_portfolio_hold_em_df[fund_portfolio_hold_em_df['季度'] == latest_quarter]
    except KeyError:
        # 如果季度列名有问题，使用找到的可能列名
        possible_date_cols = [col for col in fund_portfolio_hold_em_df.columns if '季度' in col or 'date' in col.lower() or 'period' in col.lower()]
        if possible_date_cols:
            filtered_df = fund_portfolio_hold_em_df[fund_portfolio_hold_em_df[possible_date_cols[0]] == latest_quarter]
        else:
            print("无法找到季度列进行过滤")
            return None

    # 获取前十大重仓股
    top_10_stocks = filtered_df.head(10)

    # 获取占净值比例列
    proportion_col = None
    for col in fund_portfolio_hold_em_df.columns:
        if '占净值比例' in str(col) or '净值比例' in str(col) or str(col).endswith('净值比例'):
            proportion_col = col
            break

    # 如果还是找不到，尝试使用列索引（通常是第4列，索引为3）
    if proportion_col is None:
        try:
            proportion_col = fund_portfolio_hold_em_df.columns[3]
        except IndexError:
            print("无法确定占净值比例列")
            return None

    # 计算前十大重仓股的占净值比例之和
    try:
        weight_sum = top_10_stocks[proportion_col].sum()
    except KeyError:
        print(f"无法访问列 {proportion_col}")
        return None

    print(f"\n前十大重仓股占净值比例之和: {weight_sum:.2f}%")

    return weight_sum

def analyze_funds():
    zzqz ={"近3年":24.44,"近2年":33.86,"近1年":21.80,"今年来":21.80,"近6月":19.48,"近3月":0.74,"近1月":3.25,"近1周":1.50}

    fund_open_fund_rank_em_df = ak.fund_open_fund_rank_em(symbol="全部")
    # 过滤掉"近3月"为空的数据
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df.dropna(subset=['近6月']).copy()
    # 筛选fund_open_fund_rank_em_df中近1年和近6月都大于zzqz的基金
    exclude_keywords = ["持有", "A", "通信", "期货", "有色", "黄金", "半导体", "芯片", "云计算", "商品", "创业板",
                        "中证资源", "电信", "物联网", "工程机械", "医药生物", "稀有金属", "科创板",
                        "科创创业", "人工智能", "上海金", "TMT", "指数", "可转债", "债券", "化工", "碳中和", "ESG", "ETF"]
    for keyword in tqdm(exclude_keywords):
        fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[~fund_open_fund_rank_em_df["基金简称"].str.contains(keyword, na=False)].copy()
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近1年"] >= zzqz["近1年"]].copy()
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近6月"] >= zzqz["近6月"]].copy()
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近3月"] >= zzqz["近3月"]].copy()
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近1月"] >= zzqz["近1月"]].copy()
    
    # 计算超额收益
    fund_open_fund_rank_em_df.loc[:, "近1年超额"] = fund_open_fund_rank_em_df["近1年"] - zzqz["近1年"]
    fund_open_fund_rank_em_df.loc[:, "近6月超额"] = fund_open_fund_rank_em_df["近6月"] - zzqz["近6月"]
    fund_open_fund_rank_em_df.loc[:, "近3月超额"] = fund_open_fund_rank_em_df["近3月"] - zzqz["近3月"]
    fund_open_fund_rank_em_df.loc[:, "近1月超额"] = fund_open_fund_rank_em_df["近1月"] - zzqz["近1月"]

    fund_open_fund_rank_em_df.loc[:, "成立时间"] = ""
    fund_open_fund_rank_em_df.loc[:, "最新规模"] = ""
    fund_open_fund_rank_em_df.loc[:, "换手率"] = ""
    fund_open_fund_rank_em_df.loc[:, "前10大重仓股占比"] = ""
    fund_open_fund_rank_em_df.loc[:, "持股行业集中度"] = ""
    
    # 筛选换手率大于200%的基金
    def filter_by_turnover(turnover_str):
        if pd.isna(turnover_str) or turnover_str == "" or turnover_str == "获取失败":
            return True  # 保留空值
        if "%" in turnover_str:
            turnover_value = float(turnover_str.replace("%", ""))
            return turnover_value >= 200  # 保留换手率大于等于200%的基金
        return False  # 默认不保留
    
    for idx, row in tqdm(fund_open_fund_rank_em_df.iterrows(), total=fund_open_fund_rank_em_df.shape[0]):
        code = row["基金代码"]
        # 1. 根据基金代码查找基金名称
        fund_name = get_fund_name_by_code(str(code))
        if not fund_name:
            print(f"无法获取基金 {code} 的名称")
            return None
        print(f"基金名称: {fund_name} 基金代码: {code}")
        base_name = fund_name
        if fund_name.endswith('A') or fund_name.endswith('C'):
            base_name = fund_name[:-1]
        # 2. 根据基金名称查找对应的基金代码(A+C)
        # [{'code': '015381', 'name': '东方兴瑞趋势领航混合A'}, {'code': '015382', 'name': '东方兴瑞趋势领航混合C'}]
        code_names = fetch_and_parse_fund_search(base_name)
        # 检查返回结果是否为错误信息或者不是预期的列表格式
        if isinstance(code_names, dict) and 'error' in code_names:
            print(f"获取基金 {base_name} 搜索结果失败: {code_names.get('error_message', '未知错误')}")
            continue

        if not isinstance(code_names, list):
            print(f"基金 {base_name} 搜索结果格式异常: {type(code_names)}")
            continue
        # 3. 获取A类和C类基金的规模数据
        for code_name in code_names:
            # 确保code_name是一个字典并且包含'code'键
            if not isinstance(code_name, dict) or 'code' not in code_name:
                print(f"基金代码信息格式异常: {code_name}")
                continue
            info = get_fund_info(code_name['code'])

            # 解析并累加规模数据
            scale_match = re.search(r'([\d.]+)亿元', info['最新规模'])
            if scale_match:
                scale_value = float(scale_match.group(1))
                # 如果当前值是空字符串，则初始化为0
                current_scale = fund_open_fund_rank_em_df.loc[idx, "最新规模"]
                if current_scale is not None == "":
                    current_scale = 0
                else:
                    # 提取当前值中的数字部分
                    current_match = re.search(r'([\d.]+)亿元', str(current_scale))
                    if current_match:
                        current_scale = float(current_match.group(1))
                    else:
                        current_scale = 0
                # 累加规模值
                new_scale = current_scale + scale_value
                fund_open_fund_rank_em_df.loc[idx, "最新规模"] = f"{new_scale:.2f}亿元"
        try:
            info = get_fund_info(code)
            # 使用.at代替.loc来设置单个值
            fund_open_fund_rank_em_df.at[idx, "成立时间"] = info['成立时间']
            # fund_open_fund_rank_em_df.at[idx, "最新规模"] = info['最新规模']
            # 获取换手率和重仓股信息
            fund_detail = parse_fund_data(code)
            if fund_detail:
                if '换手率' in fund_detail:
                    fund_open_fund_rank_em_df.at[idx, "换手率"] = fund_detail['换手率']
                if '前10大重仓股占比' in fund_detail:
                    fund_open_fund_rank_em_df.at[idx, "前10大重仓股占比"] = fund_detail['前10大重仓股占比']
                if '持股行业集中度' in fund_detail:
                    fund_open_fund_rank_em_df.at[idx, "持股行业集中度"] = fund_detail['持股行业集中度']

            # 如果九泉数据API没有返回前10大重仓股占比，尝试使用akshare获取
            if pd.isna(fund_open_fund_rank_em_df.at[idx, "前10大重仓股占比"]) or fund_open_fund_rank_em_df.at[idx, "前10大重仓股占比"] == "":
                top10_weight = get_top10_stocks_weight_robust(code)
                if top10_weight is not None:
                    fund_open_fund_rank_em_df.at[idx, "前10大重仓股占比"] = f"{top10_weight:.2f}%"
                    print(f"使用akshare获取到前10大重仓股占比: {top10_weight:.2f}%")
                else:
                    print(f"akshare也无法获取基金{code}的前10大重仓股占比")
        except Exception as e:
            print(f"基金代码{code}查询失败: {e}")
    
    # 过滤掉换手率大于200%的基金
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df["换手率"].apply(filter_by_turnover)
    ].copy()
    
    # 过滤掉最新规模大于40亿或小于2000万的基金，最新规模数据可能为空，"40亿"，"40万"
    def filter_by_scale(scale_str):
        if pd.isna(scale_str) or scale_str == "" or scale_str == "获取失败":
            return True  # 保留空值
        try:
            if "亿" in scale_str:
                scale_value = float(scale_str.replace("亿", ""))
                # 保留2000万(0.2亿)到40亿之间的基金
                return 0.2 <= scale_value <= 40
            elif "万" in scale_str:
                scale_value = float(scale_str.replace("万", ""))
                # 2000万 = 0.2亿，只保留大于等于2000万的基金
                return scale_value >= 2000
            else:
                # 处理类似"0.39元"这样的数据
                scale_value = float(re.search(r'[\d.]+', scale_str).group())
                # 假设没有单位标识的数字是以亿元为单位
                return 0.2 <= scale_value <= 40
        except (ValueError, AttributeError):
            # 如果解析失败，保留该基金
            return True
        return False  # 默认不保留
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df["最新规模"].apply(filter_by_scale)
    ].copy()
    
    # 过滤掉前10大重仓股占比大于等于40%的基金
    def filter_by_top10_holdings(holdings_str):
        if pd.isna(holdings_str) or holdings_str == "" or holdings_str == "获取失败":
            return True  # 保留空值
        if "%" in holdings_str:
            holdings_value = float(holdings_str.replace("%", ""))
            return holdings_value < 40  # 保留占比小于40%的基金
        return False  # 默认不保留
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df["前10大重仓股占比"].apply(filter_by_top10_holdings)
    ].copy()

    # 按照比例关系筛选超额收益
    # 如果近1年超额收益是20%，则近6月超额收益要大于10%，近3月超额收益要大于5%，近1月超额收益要大于1.6%
    def filter_by_excess_return_ratio(row):
        # 获取基金的超额收益
        excess_1y = row["近1年超额"]
        excess_6m = row["近6月超额"]
        excess_3m = row["近3月超额"]
        excess_1m = row["近1月超额"]

        # 如果近1年超额收益大于0，应用比例筛选规则
        if excess_1y > 0:
            # 计算比例阈值
            threshold_6m = excess_1y * 0.5  # 6个月相对于1年的比例
            threshold_3m = excess_1y * 0.25  # 3个月相对于1年的比例
            threshold_1m = excess_1y * 0.08  # 1个月相对于1年的比例（按年化估算）

            # 检查是否满足比例关系
            return excess_6m >= threshold_6m and excess_3m >= threshold_3m and excess_1m >= threshold_1m
        else:
            # 如果近1年超额收益不大于0，则不应用比例筛选规则，保留基金
            return True

    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df.apply(filter_by_excess_return_ratio, axis=1)
    ].copy()

    fund_open_fund_rank_em_df.to_excel("fund_open_fund_rank_em.xlsx")
    

if __name__ == "__main__":
    analyze_funds()