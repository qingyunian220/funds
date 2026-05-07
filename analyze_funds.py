import re
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from tqdm import tqdm
from openpyxl import load_workbook
from openpyxl.styles import Alignment
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
        try:
            fund_portfolio_hold_em_df = ak.fund_portfolio_hold_em(symbol=fund_code, date=previous_year)
        except KeyError as e2:
            return None

    if fund_portfolio_hold_em_df is None or fund_portfolio_hold_em_df.empty:
        return None

    # 获取所有不同的季度
    try:
        quarters = fund_portfolio_hold_em_df['季度'].unique()
    except KeyError:
        possible_date_cols = [col for col in fund_portfolio_hold_em_df.columns if '季度' in col or 'date' in col.lower() or 'period' in col.lower()]
        if possible_date_cols:
            quarters = fund_portfolio_hold_em_df[possible_date_cols[0]].unique()
        else:
            return None

    # 确定最新的季度
    latest_quarter = sorted(quarters, reverse=True)[0]

    # 提取最近一个季度的数据
    try:
        filtered_df = fund_portfolio_hold_em_df[fund_portfolio_hold_em_df['季度'] == latest_quarter]
    except KeyError:
        possible_date_cols = [col for col in fund_portfolio_hold_em_df.columns if '季度' in col or 'date' in col.lower() or 'period' in col.lower()]
        if possible_date_cols:
            filtered_df = fund_portfolio_hold_em_df[fund_portfolio_hold_em_df[possible_date_cols[0]] == latest_quarter]
        else:
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
            return None

    # 计算前十大重仓股的占净值比例之和
    try:
        weight_sum = top_10_stocks[proportion_col].sum()
    except KeyError:
        return None

    return weight_sum


def get_csi_all_share_returns():
    try:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y%m%d')

        df = ak.stock_zh_index_hist_csindex(symbol='000985', start_date=start_date, end_date=end_date)

        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期', ascending=False).reset_index(drop=True)

        latest_date = df.iloc[0]['日期']
        latest_value = df.iloc[0]['收盘']

        period_mapping = {
            '近1月': 30,
            '近3月': 90,
            '近6月': 180,
            '近1年': 365,
            '近2年': 365 * 2,
            '近3年': 365 * 3
        }

        period_returns = {}

        for period_name, days in period_mapping.items():
            target_date = latest_date - timedelta(days=days)
            target_row = df.iloc[(df['日期'] - target_date).abs().argsort()[:1]]

            if not target_row.empty:
                target_value = target_row.iloc[0]['收盘']
                return_rate = (latest_value - target_value) / target_value * 100
                period_returns[period_name] = round(return_rate, 2)
            else:
                period_returns[period_name] = 0

        start_of_year = datetime(latest_date.year, 1, 1)
        target_row = df.iloc[(df['日期'] - start_of_year).abs().argsort()[:1]]
        if not target_row.empty:
            target_value = target_row.iloc[0]['收盘']
            return_rate = (latest_value - target_value) / target_value * 100
            period_returns['今年来'] = round(return_rate, 2)
        else:
            period_returns['今年来'] = 0

        print(f"中证全指(000985)最新日期：{latest_date.strftime('%Y-%m-%d')}")
        print(f"中证全指各时间段收益率：{period_returns}")

        return period_returns

    except Exception as e:
        print(f"自动获取中证全指数据失败：{e}")
        print("使用默认数据...")
        return {"近3年":25.33,"近2年":49.90,"近1年":33.97,"今年来":5.95,"近6月":8.63,"近3月":-1.35,"近1月":9.57}


def filter_by_excess_return_ratio(row):
    """
    判断超额收益曲线是否"平稳向上"
    
    算法：胜率 + 波动率
    1. 胜率：上升的时间段占比
    2. 波动率：各时间段超额收益的标准差
    3. 综合判断
    
    根据用户例子：
    - A基金：71.65% → 35.62% → 15.68% → 9.09% → 应该通过（都是正的）
    - B基金：3.23% → -5.06% → -5.25% → -4.13% → 应该不通过（持续恶化且变负）
    """
    excess_1y = row["近1年超额"]
    excess_6m = row["近6月超额"]
    excess_3m = row["近3月超额"]
    excess_1m = row["近1月超额"]
    
    # ========== 收集有效数据 ==========
    if pd.isna(excess_1y):
        # 不满一年：只用近6月、近3月、近1月
        excess_list = [excess_6m, excess_3m, excess_1m]
        # 计算年化
        annualized_6m = excess_6m * 2
        annualized_3m = excess_3m * 4
        annualized_1m = excess_1m * 12
        annualized_list = [annualized_6m, annualized_3m, annualized_1m]
    else:
        # 满一年：用全部四个
        excess_list = [excess_1y, excess_6m, excess_3m, excess_1m]
        annualized_1y = excess_1y
        annualized_6m = excess_6m * 2
        annualized_3m = excess_3m * 4
        annualized_1m = excess_1m * 12
        annualized_list = [annualized_1y, annualized_6m, annualized_3m, annualized_1m]
    
    # ========== 指标1：胜率 ==========
    # 计算有多少个时间段是正的
    positive_count = sum(1 for x in excess_list if x > 0)
    win_rate = positive_count / len(excess_list)
    
    # 计算上升的时间段（后面比前面大）
    up_count = 0
    total_compare_pairs = 0
    for i in range(len(excess_list) - 1):
        if excess_list[i + 1] > excess_list[i]:
            up_count += 1
        total_compare_pairs += 1
    improving_rate = up_count / total_compare_pairs if total_compare_pairs > 0 else 0
    
    # ========== 指标2：波动率（标准差） ==========
    # 计算各时间段超额收益的标准差（越小越平稳）
    volatility = np.std(excess_list)
    
    # ========== 指标3：年化收益的稳定性 ==========
    annualized_volatility = np.std(annualized_list)
    
    # ========== 坏的情况（直接不通过） ==========
    # 持续恶化且变负
    if len(excess_list) >= 3:
        if pd.isna(excess_1y):
            # 不满一年的情况
            getting_worse_and_negative = (excess_6m < 0) and (excess_3m < 0) and (excess_1m < 0)
        else:
            getting_worse_and_negative = (excess_1y > 0) and (excess_6m < 0) and (excess_3m < 0)
        
        if getting_worse_and_negative:
            return False
    
    # ========== 好的情况（通过） ==========
    # 条件1：高胜率（大部分时间段都是正的
    high_win_rate = win_rate >= 0.6  # 60%以上是正的
    
    # 条件2：有改善趋势（上升的时间段多）
    has_improvement = improving_rate >= 0.4  # 40%以上的比较是上升的
    
    # 条件3：波动率适中（不是剧烈波动
    low_volatility = volatility < 50  # 标准差小于50%（避免暴涨暴跌）
    
    # 条件4：所有都是正的
    all_positive = all(x > 0 for x in excess_list)
    
    # 条件5：近期表现好
    good_recent = annualized_list[-1] > annualized_list[-2] if len(annualized_list) >= 2 else False
    
    # 组合判断：满足至少两个条件就通过
    score = sum([high_win_rate, has_improvement, low_volatility, all_positive, good_recent])
    
    return score >= 2


def fetch_fund_details(row):
    """单只基金的详细数据获取（用于多线程）"""
    code = row["基金代码"]
    fund_name = row["基金简称"]
    base_name = fund_name[:-1] if (fund_name.endswith('A') or fund_name.endswith('C')) else fund_name
    
    result = {
        "idx": row.name,
        "成立时间": "",
        "最新规模": "",
        "换手率": "",
        "前10大重仓股占比": "",
        "持股行业集中度": ""
    }
    
    # 先获取当前基金的基本信息和规模
    try:
        info = get_fund_info(code)
        result["成立时间"] = info['成立时间']
        
        # 先获取当前基金的规模作为基础（支持"亿元"或"亿"格式）
        scale_match = re.search(r'([\d.]+)亿', info['最新规模'])
        total_scale = 0
        if scale_match:
            total_scale = float(scale_match.group(1))
            result["最新规模"] = f"{total_scale:.2f}亿元"
        
        # 再尝试获取A类和C类基金的规模数据并累加
        try:
            code_names = fetch_and_parse_fund_search(base_name)
            if isinstance(code_names, list):
                for code_name in code_names:
                    if isinstance(code_name, dict) and 'code' in code_name and code_name['code'] != code:  # 排除当前基金自己
                        try:
                            info_ac = get_fund_info(code_name['code'])
                            scale_match_ac = re.search(r'([\d.]+)亿', info_ac['最新规模'])
                            if scale_match_ac:
                                total_scale += float(scale_match_ac.group(1))
                                result["最新规模"] = f"{total_scale:.2f}亿元"
                        except:
                            pass
        except Exception as e:
            pass
    except Exception as e:
        pass
    
    # 获取其他详细信息
    try:
        fund_detail = parse_fund_data(code)
        if fund_detail:
            if '换手率' in fund_detail:
                result["换手率"] = fund_detail['换手率']
            if '前10大重仓股占比' in fund_detail:
                result["前10大重仓股占比"] = fund_detail['前10大重仓股占比']
            if '持股行业集中度' in fund_detail:
                result["持股行业集中度"] = fund_detail['持股行业集中度']
        
        if not result["前10大重仓股占比"] or pd.isna(result["前10大重仓股占比"]):
            top10_weight = get_top10_stocks_weight_robust(code)
            if top10_weight is not None:
                result["前10大重仓股占比"] = f"{top10_weight:.2f}%"
    except Exception as e:
        pass
    
    return result


def analyze_funds():
    zzqz = get_csi_all_share_returns()

    print("正在获取基金排名数据...")
    fund_open_fund_rank_em_df = ak.fund_open_fund_rank_em(symbol="全部")
    print(f"step1_原始数据：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step1_原始数据.xlsx", index=False)
    
    # 第一步：先做不需要API的快速筛选
    print("正在进行初步筛选...")
    
    # 过滤掉数据为空的（近1年可以为空）
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df.dropna(subset=['近6月', '近3月', '近1月']).copy()
    print(f"step2_去空值后：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step2_去空值.xlsx", index=False)
    
    # 关键词过滤
    exclude_keywords = ["持有", "A", "通信", "期货", "有色", "黄金", "半导体", "芯片", "云计算", "商品", "创业板",
                        "中证资源", "电信", "物联网", "工程机械", "医药生物", "稀有金属", "科创板",
                        "科创创业", "人工智能", "上海金", "TMT", "指数", "可转债", "债券", "化工", "碳中和", "ESG", "ETF"]
    
    mask = ~fund_open_fund_rank_em_df["基金简称"].str.contains('|'.join(exclude_keywords), na=False)
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[mask].copy()
    print(f"step3_关键词过滤后：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step3_关键词过滤.xlsx", index=False)
    
    # 业绩筛选（近1年不强制要求，保留近6月/3月/1月）
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        (fund_open_fund_rank_em_df["近6月"] >= zzqz["近6月"]) &
        (fund_open_fund_rank_em_df["近3月"] >= zzqz["近3月"]) &
        (fund_open_fund_rank_em_df["近1月"] >= zzqz["近1月"])
    ].copy()
    print(f"step4_业绩筛选后：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step4_业绩筛选.xlsx", index=False)
    
    # 计算超额收益
    fund_open_fund_rank_em_df.loc[:, "近1年超额"] = fund_open_fund_rank_em_df.apply(
        lambda row: row["近1年"] - zzqz["近1年"] if pd.notna(row["近1年"]) else None, 
        axis=1
    )
    fund_open_fund_rank_em_df.loc[:, "近6月超额"] = fund_open_fund_rank_em_df["近6月"] - zzqz["近6月"]
    fund_open_fund_rank_em_df.loc[:, "近3月超额"] = fund_open_fund_rank_em_df["近3月"] - zzqz["近3月"]
    fund_open_fund_rank_em_df.loc[:, "近1月超额"] = fund_open_fund_rank_em_df["近1月"] - zzqz["近1月"]
    
    # 超额收益比例筛选
    # fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
    #     fund_open_fund_rank_em_df.apply(filter_by_excess_return_ratio, axis=1)
    # ].copy()
    # print(f"step5_超额收益筛选后：{len(fund_open_fund_rank_em_df)} 只基金")
    # fund_open_fund_rank_em_df.to_excel("step5_超额收益筛选.xlsx", index=False)
    
    # 第二步：使用多线程补充详细数据
    fund_open_fund_rank_em_df.loc[:, "成立时间"] = ""
    fund_open_fund_rank_em_df.loc[:, "最新规模"] = ""
    fund_open_fund_rank_em_df.loc[:, "换手率"] = ""
    fund_open_fund_rank_em_df.loc[:, "前10大重仓股占比"] = ""
    fund_open_fund_rank_em_df.loc[:, "持股行业集中度"] = ""
    
    print(f"正在使用多线程补充基金详细数据（线程数：20）...")
    
    # 使用ThreadPoolExecutor并行获取数据
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 提交所有任务
        futures = {executor.submit(fetch_fund_details, row): idx for idx, row in fund_open_fund_rank_em_df.iterrows()}
        
        # 收集结果
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                result = future.result()
                idx = result["idx"]
                fund_open_fund_rank_em_df.at[idx, "成立时间"] = result["成立时间"]
                fund_open_fund_rank_em_df.at[idx, "最新规模"] = result["最新规模"]
                fund_open_fund_rank_em_df.at[idx, "换手率"] = result["换手率"]
                fund_open_fund_rank_em_df.at[idx, "前10大重仓股占比"] = result["前10大重仓股占比"]
                fund_open_fund_rank_em_df.at[idx, "持股行业集中度"] = result["持股行业集中度"]
            except Exception as e:
                pass
    
    # 保存补充完详细数据的结果
    fund_open_fund_rank_em_df.to_excel("step6_补充数据完成.xlsx", index=False)
    
    # 第三步：基于补充的数据做最终筛选
    print("正在进行最终筛选...")
    
    # 打印规模数据统计
    scale_data = fund_open_fund_rank_em_df["最新规模"].values
    empty_count = sum(1 for s in scale_data if s == "" or pd.isna(s))
    print(f"规模数据情况：总共 {len(scale_data)} 只，空数据 {empty_count} 只，有数据 {len(scale_data) - empty_count} 只")
    if len(scale_data) - empty_count > 0:
        sample_scales = [s for s in scale_data if s != "" and not pd.isna(s)][:10]
        print(f"规模数据示例：{sample_scales}")
    
    # 换手率筛选
    def filter_by_turnover(turnover_str):
        if pd.isna(turnover_str) or turnover_str == "" or turnover_str == "获取失败":
            return True
        if "%" in turnover_str:
            turnover_value = float(turnover_str.replace("%", ""))
            return turnover_value >= 500
        return False
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df["换手率"].apply(filter_by_turnover)
    ].copy()
    print(f"step7_换手率筛选后：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step7_换手率筛选.xlsx", index=False)
    
    # 规模筛选
    debug_scale_results = []
    def filter_by_scale(scale_str):
        try:
            if pd.isna(scale_str) or scale_str == "" or scale_str == "获取失败":
                debug_scale_results.append((scale_str, "空数据"))
                return False  # 空数据直接排除
            
            # 清理一下字符串
            scale_str_clean = str(scale_str).strip()
            
            # 统一转换为亿元单位
            if "亿" in scale_str_clean:
                # 提取数字部分
                num_match = re.search(r'([\d.]+)', scale_str_clean)
                if num_match:
                    scale_value = float(num_match.group(1))
                else:
                    debug_scale_results.append((scale_str, "无法提取数字"))
                    return False
            elif "万" in scale_str_clean:
                # 万元转亿元：除以10000
                num_match = re.search(r'([\d.]+)', scale_str_clean)
                if num_match:
                    scale_value = float(num_match.group(1)) / 10000
                else:
                    debug_scale_results.append((scale_str, "无法提取数字"))
                    return False
            else:
                # 假设无单位的数据是亿元
                num_match = re.search(r'([\d.]+)', scale_str_clean)
                if num_match:
                    scale_value = float(num_match.group(1))
                else:
                    debug_scale_results.append((scale_str, "无法提取数字"))
                    return False
            
            # 判断是否在范围内
            in_range = 0.2 <= scale_value <= 10
            debug_scale_results.append((scale_str, scale_value, in_range))
            return in_range
        except Exception as e:
            debug_scale_results.append((scale_str, f"异常：{e}"))
            return False  # 解析失败排除
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df["最新规模"].apply(filter_by_scale)
    ].copy()
    print(f"step8_规模筛选后：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step8_规模筛选.xlsx", index=False)
    
    # 打印调试信息
    print("\n=== 规模筛选调试信息 ===")
    for i, res in enumerate(debug_scale_results[:30]):  # 只打印前30条
        print(f"{i+1}. {res}")
    if len(debug_scale_results) > 30:
        print(f"... 还有 {len(debug_scale_results)-30} 条")
    
    # 前10大重仓股占比筛选（<40%）
    print("\n=== 正在进行前10大重仓股筛选 ===")
    debug_holdings_results = []
    
    def filter_by_holdings(holdings_str):
        try:
            if pd.isna(holdings_str) or holdings_str == "" or holdings_str == "获取失败":
                debug_holdings_results.append((holdings_str, "空数据"))
                return True  # 空数据保留
            
            # 提取数字
            num_match = re.search(r'([\d.]+)', str(holdings_str))
            if num_match:
                holdings_value = float(num_match.group(1))
                in_range = holdings_value < 40
                debug_holdings_results.append((holdings_str, holdings_value, in_range))
                return in_range
            else:
                debug_holdings_results.append((holdings_str, "无法提取数字"))
                return True  # 无法解析的保留
        except Exception as e:
            debug_holdings_results.append((holdings_str, f"异常：{e}"))
            return True  # 异常保留
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df["前10大重仓股占比"].apply(filter_by_holdings)
    ].copy()
    print(f"step9_前10大重仓股筛选后：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step9_前10大重仓股筛选.xlsx", index=False)
    
    # 打印调试信息
    print("\n=== 前10大重仓股筛选调试信息 ===")
    for i, res in enumerate(debug_holdings_results[:30]):
        print(f"{i+1}. {res}")
    if len(debug_holdings_results) > 30:
        print(f"... 还有 {len(debug_holdings_results)-30} 条")
    
    # 持股行业集中度筛选（<40%）
    print("\n=== 正在进行持股行业集中度筛选 ===")
    debug_concentration_results = []
    
    def filter_by_concentration(concentration_str):
        try:
            if pd.isna(concentration_str) or concentration_str == "" or concentration_str == "获取失败":
                debug_concentration_results.append((concentration_str, "空数据"))
                return True  # 空数据保留
            
            # 提取数字
            num_match = re.search(r'([\d.]+)', str(concentration_str))
            if num_match:
                concentration_value = float(num_match.group(1))
                in_range = concentration_value < 40
                debug_concentration_results.append((concentration_str, concentration_value, in_range))
                return in_range
            else:
                debug_concentration_results.append((concentration_str, "无法提取数字"))
                return True  # 无法解析的保留
        except Exception as e:
            debug_concentration_results.append((concentration_str, f"异常：{e}"))
            return True  # 异常保留
    
    fund_open_fund_rank_em_df = fund_open_fund_rank_em_df[
        fund_open_fund_rank_em_df["持股行业集中度"].apply(filter_by_concentration)
    ].copy()
    print(f"step10_持股行业集中度筛选后：{len(fund_open_fund_rank_em_df)} 只基金")
    fund_open_fund_rank_em_df.to_excel("step10_持股行业集中度筛选.xlsx", index=False)
    
    # 打印调试信息
    print("\n=== 持股行业集中度筛选调试信息 ===")
    for i, res in enumerate(debug_concentration_results[:30]):
        print(f"{i+1}. {res}")
    if len(debug_concentration_results) > 30:
        print(f"... 还有 {len(debug_concentration_results)-30} 条")
    
    print(f"\n最终筛选后剩余 {len(fund_open_fund_rank_em_df)} 只基金")
    
    # 删除不需要的列
    columns_to_drop = ["序号", "单位净值", "累计净值", "日增长率", "自定义", "手续费"]
    # 只删除实际存在的列
    existing_columns = [col for col in columns_to_drop if col in fund_open_fund_rank_em_df.columns]
    df_export = fund_open_fund_rank_em_df.drop(columns=existing_columns, errors='ignore')
    
    # 按近6月超额倒序排序
    if "近6月超额" in df_export.columns:
        df_export = df_export.sort_values(by="近6月超额", ascending=False)
    
    # 使用openpyxl优化Excel格式
    
    # 保存到两个文件
    output_files = ["step11_最终结果.xlsx", "fund_open_fund_rank_em.xlsx"]
    
    for file in output_files:
        df_export.to_excel(file, index=False)
        
        # 打开文件并优化格式
        wb = load_workbook(file)
        ws = wb.active
        
        # 设置所有单元格居中对齐
        alignment = Alignment(horizontal='center', vertical='center')
        for row in ws.iter_rows():
            for cell in row:
                cell.alignment = alignment
        
        # 自适应列宽（紧凑版）
        for col in ws.columns:
            max_length = 0
            column = col[0].column_letter  # 获取列字母
            column_name = col[0].value
            
            for cell in col:
                try:
                    # 计算字符长度（中文字符按1.5个宽度算，更贴近Excel实际显示）
                    cell_value = str(cell.value)
                    length = 0
                    for char in cell_value:
                        if '\u4e00' <= char <= '\u9fff':  # 中文字符
                            length += 1.5
                        else:
                            length += 1
                    if length > max_length:
                        max_length = length
                except:
                    pass
            
            # 计算列宽（更紧凑）
            adjusted_width = max_length + 1  # 加1个字符的padding就够
            
            # 特殊列适当加宽，但不要太大
            if column_name == "基金简称":
                adjusted_width = max(adjusted_width, 12)  # 基金简称至少12
            elif column_name == "基金代码":
                adjusted_width = max(adjusted_width, 10)  # 基金代码至少10
            elif "超额" in str(column_name) or "收益率" in str(column_name):
                adjusted_width = max(adjusted_width, 10)
            
            # 最大宽度限制（缩紧到40）
            adjusted_width = min(adjusted_width, 40)
            
            ws.column_dimensions[column].width = adjusted_width
        
        wb.save(file)
    
    print("结果已保存到 fund_open_fund_rank_em.xlsx（格式已优化）")


if __name__ == "__main__":
    analyze_funds()
