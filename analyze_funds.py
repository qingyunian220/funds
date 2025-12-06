import re

import pandas as pd
import akshare as ak
from tqdm import tqdm
from enhanced_index import get_fund_info
from fund_data_processor import get_fund_name_by_code
from fund_search_parser import fetch_and_parse_fund_search
from jiuquan_fund import parse_fund_data

def analyze_funds():
    zzqz ={"近3年":17.02,"近2年":21.91,"近1年":18.17,"今年来":18.06,"近6月":17.54,"近3月":-0.21,"近1月":-2.84,"近1周":-4.49}

    fund_open_fund_rank_em_df = ak.fund_open_fund_rank_em(symbol="全部")
    # 过滤掉"近3月"为空的数据
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df.dropna(subset=['近6月']).copy()
    # 筛选fund_open_fund_rank_em_df中近1年和近6月都大于zzqz的基金
    exclude_keywords = ["持有","A"]
    for keyword in tqdm(exclude_keywords):
        fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[~fund_open_fund_rank_em_df["基金简称"].str.contains(keyword, na=False)].copy()
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近1年"] >= zzqz["近1年"]].copy()
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近6月"] >= zzqz["近6月"]].copy()
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近3月"] >= zzqz["近3月"]].copy()
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["近1月"] >= zzqz["近1月"]].copy()
    
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
        print(f"基金名称: {fund_name}")
        base_name = fund_name
        if fund_name.endswith('A') or fund_name.endswith('C'):
            base_name = fund_name[:-1]
        # 2. 根据基金名称查找对应的基金代码(A+C)
        # [{'code': '015381', 'name': '东方兴瑞趋势领航混合A'}, {'code': '015382', 'name': '东方兴瑞趋势领航混合C'}]
        code_names = fetch_and_parse_fund_search(base_name)
        # 3. 获取A类和C类基金的规模数据
        for code_name in code_names:
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
    
    fund_open_fund_rank_em_df.to_excel("fund_open_fund_rank_em.xlsx")
    

if __name__ == "__main__":
    analyze_funds()