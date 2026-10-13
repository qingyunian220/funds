import re
import os
import calendar
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import akshare as ak
import time

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import schedule
import threading
from flask import Flask, jsonify, request, render_template
import os
import json
import numpy as np
from openpyxl import load_workbook

from fund_data_processor import get_fund_name_by_code
from fund_search_parser import fetch_and_parse_fund_search
from jiuquan_fund import parse_fund_data
from jiuquaner_fund_style import get_fund_list, batch_get_style

# 绘图相关（参考 analyze_funds.py 计算超额收益曲线）
import matplotlib
matplotlib.use('Agg')  # 非交互式后端，避免无显示环境报错
import matplotlib.pyplot as plt

# 设置中文字体，避免图表中文乱码
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

from analyze_funds import get_fund_nav_history, calculate_excess_return_curve, calculate_stability_score

# 当前选中的基金类型（由 update_fund_data 设置，供 app.py 读取控制页面显示）
SELECTED_FUND_TYPES = ["沪深300", "A500", "中证500", "中证1000", "国证2000", "小微盘", "大盘主动"]

# 市值评分筛选类基金配置：基金类型 -> (市值评分阈值, 比较方向 'lt' 评分<阈值 / 'gt' 评分>阈值)
SPECIAL_MARKET_CAP_FILTERS = {
    "小微盘": (25, 'lt'),
    "大盘主动": (90, 'gt'),
}
# 市值评分筛选类基金对应的超额收益基准基金代码
SPECIAL_FUND_BENCHMARKS = {
    "小微盘": "320016",
    "大盘主动": "001638",
}
# 市值评分筛选类基金的最新规模上限（亿元）：达到上限的基金剔除，无规模数据的保留，未配置的类型不筛选规模
SPECIAL_FUND_MAX_SCALE = {
    "大盘主动": 10,
}
# 市值评分筛选类基金的附加筛选条件：min_stability_score 为超额收益曲线稳定性评分下限(0-100，参考 analyze_funds.py)，
# max_top10_ratio 为前10大重仓股占比上限(%)，max_industry_concentration 为持股行业集中度上限(%)；未配置的类型不生效
# skip_top_period_filter 为 True 时跳过"各时间段前10"筛选（保留全部候选），如大盘主动
SPECIAL_FUND_EXTRA_FILTERS = {
    "大盘主动": {
        "min_stability_score": 50,
        "max_top10_ratio": 80,
        "max_industry_concentration": 80,
        "skip_top_period_filter": True,
    },
}

# 业绩筛选参与的时间段选项（"近2年" = 当前系统日期往前完整24个自然月；
# 排序按时间跨度由短到长，"今年来"为年度特殊项收尾）
SCREEN_PERIOD_COLUMNS = ['近1月', '近3月', '近6月', '近1年', '近2年', '今年来']

# 指数类基金（沪深300、A500、中证500、中证1000、国证2000等）前10大重仓股占比上限(%)：
# 占比超过该值的基金剔除，数据缺失的保留（与既有规模/占比筛选口径一致）
INDEX_FUND_MAX_TOP10_RATIO = 50


def get_recent_two_year_window(reference=None):
    """返回(起始日, 截止日)：从当前系统日期（或指定日期）往前推算完整的24个自然月。

    起始日的"日"按目标月月末钳制，例如基准日为 2024-02-29 时起始日为 2022-02-28，
    保证得到的是完整的24个月区间而非简单的730天。

    参数
    ----------
    reference : datetime.date, optional
        基准截止日，默认取 date.today()

    返回
    -------
    tuple[date, date]
        (起始日, 截止日)
    """
    end = reference or date.today()
    start_year, start_month = end.year - 2, end.month
    start_day = min(end.day, calendar.monthrange(start_year, start_month)[1])
    return date(start_year, start_month, start_day), end


def select_top_indices_by_periods(fund_df, period_columns=None, top_n=10):
    """在任一时间段收益进入前 top_n 的基金索引集合（各时间段取并集）。

    - 空值/非数值（如运作不满24个月基金的"近2年"）会被自动跳过，
      即"近2年"通道只会纳入时间字段（成立时间）落在近2年区间内的基金；
    - 仅对 object 类型列做数值转换，数值型列保持原行为，确保与既有逻辑一致。

    参数
    ----------
    fund_df : pd.DataFrame
        基金排名数据
    period_columns : list[str], optional
        参与筛选的时间段列，默认使用 SCREEN_PERIOD_COLUMNS（含"近2年"）
    top_n : int
        每个时间段保留的前 N 名，默认10

    返回
    -------
    set
        命中任一时间段前 N 名的行索引集合
    """
    columns = period_columns if period_columns is not None else SCREEN_PERIOD_COLUMNS
    if '近2年' in columns:
        window_start, window_end = get_recent_two_year_window()
        print(f"时间段筛选：'近2年'按当前系统日期前推完整24个月动态匹配，"
              f"区间 {window_start.strftime('%Y-%m-%d')} ~ {window_end.strftime('%Y-%m-%d')}")

    top_indices = set()
    for col in columns:
        if col in fund_df.columns:
            series = fund_df[col]
            if series.dtype == object:
                series = pd.to_numeric(series, errors='coerce')
            top_indices.update(series.nlargest(top_n).index)
    return top_indices


# # Flask应用配置
# app = Flask(__name__)
# app.config['JSON_AS_ASCII'] = False
#
# # 设置Excel文件路径
# EXCEL_FILE = 'index-fund.xlsx'

def get_fund_info(fund_code):
    """
    通过天天基金接口获取基金的成立时间和最新规模（优化版）

    该方法只请求一次基金详情页，并使用 BeautifulSoup 解析 HTML，
    同时提取成立日期和最新规模，效率更高。

    参数
    ----------
    fund_code : str
        基金代码，如 '001186'

    返回
    -------
    dict
        {
            '基金代码': str,
            '成立日期': str,
            '最新规模': str,
            '规模日期': str,
            '错误': str  # 如果获取失败
        }
    """
    result = {
        '基金代码': fund_code,
        '成立日期': '',
        '最新规模': '',
        '规模日期': '',
        '错误': None
    }

    # 请求基金详情页
    detail_url = f'http://fund.eastmoney.com/{fund_code}.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(detail_url, headers=headers, timeout=10)
        response.encoding = 'utf-8'

        # 如果请求失败（如404），则直接返回
        if response.status_code != 200:
            result['错误'] = f"请求失败，状态码: {response.status_code}"
            return result

        # 使用 BeautifulSoup 解析 HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # 查找包含基金信息的表格
        info_table = soup.find('div', class_='infoOfFund')
        if not info_table:
            result['错误'] = "未找到基金信息区域（.infoOfFund）"
            return result

        # 遍历表格中的所有单元格
        for td in info_table.find_all('td'):
            td_text = td.get_text(strip=True)

            # 提取成立日期
            if '成 立 日' in td_text:
                # 使用正则表达式提取日期，更精确
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', td_text)
                if date_match:
                    result['成立时间'] = date_match.group(1)
            # 提取规模和规模日期
            if '规模' in td_text and '亿元' in td_text:
                # 使用正则表达式提取规模和日期
                scale_match = re.search(r'([\d.]+)亿元（(\d{4}-\d{2}-\d{2})）', td_text)
                if scale_match:
                    result['最新规模'] = f"{scale_match.group(1)}亿元"
                    result['最新规模日期'] = scale_match.group(2)

    except requests.exceptions.RequestException as e:
        result['错误'] = f"网络请求错误: {e}"
    except Exception as e:
        result['错误'] = f"解析错误: {e}"

    return result

def fetch_fund_data(fund_type):
    """获取指定类型基金数据（只保留各时间段前10名的基金）"""
    try:
        # 获取所有基金基础信息
        fund_open_fund_rank_em_df = ak.fund_open_fund_rank_em(symbol="全部")
        fund_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["基金简称"].str.contains(fund_type, na=False)]
        fund_df = fund_df[fund_df["基金简称"].str.contains("C", na=False)].copy()

        # 过滤掉"近6月"为空的数据
        fund_df = fund_df.dropna(subset=['近6月']).copy()

        exclude_keywords = ["红利", "基本面", "价值", "非银", "成长", "低波动","信息技术","周期","非周期","地产","有色","医药","保险","金融","持有","自由现金流"]
        for keyword in exclude_keywords:
            fund_df = fund_df[~fund_df["基金简称"].str.contains(keyword, na=False)].copy()

        # 筛选出在各时间段任意一个进入前10的基金（含"近2年"，完整24个月动态窗口）
        top_indices = select_top_indices_by_periods(fund_df)

        fund_df = fund_df.loc[list(top_indices)].copy()
        print(f"{fund_type}基金: 从全部基金中筛选出 {len(fund_df)} 只各时间段前10基金")

        if fund_df.empty:
            return pd.DataFrame()

        fund_df.loc[:, "成立时间"] = ""
        fund_df.loc[:, "最新规模"] = ""
        fund_df.loc[:, "换手率"] = ""
        fund_df.loc[:, "前10大重仓股占比"] = ""
        fund_df.loc[:, "持股行业集中度"] = ""

        for idx, row in tqdm(fund_df.iterrows(), total=fund_df.shape[0], desc=f"获取{fund_type}基金详情"):
            code = row["基金代码"]
            # 1. 根据基金代码查找基金名称 - 即使失败也继续
            fund_name = get_fund_name_by_code(str(code))
            if not fund_name:
                print(f"无法获取基金 {code} 的名称，跳过该基金")
                continue
            base_name = fund_name
            if fund_name.endswith('A') or fund_name.endswith('C'):
                base_name = fund_name[:-1]
            # 2. 根据基金名称查找对应的基金代码(A+C)
            code_names = fetch_and_parse_fund_search(base_name)
            # 3. 获取A类和C类基金的规模数据
            if isinstance(code_names, list) and len(code_names) > 0:
                for code_name in code_names:
                    info = get_fund_info(code_name['code'])

                # 解析并累加规模数据
                scale_match = re.search(r'([\d.]+)亿元', info['最新规模'])
                if scale_match:
                    scale_value = float(scale_match.group(1))
                    # 如果当前值是空字符串，则初始化为0
                    current_scale = fund_df.loc[idx, "最新规模"]
                    if current_scale == "":
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
                    fund_df.loc[idx, "最新规模"] = f"{new_scale:.2f}亿元"
            try:
                info = get_fund_info(code)
                fund_df.loc[idx, "成立时间"] = info['成立时间']
                # 获取换手率和重仓股信息
                fund_detail = parse_fund_data(code)
                if fund_detail:
                    if '换手率' in fund_detail:
                        fund_df.loc[idx, "换手率"] = fund_detail['换手率']
                    if '前10大重仓股占比' in fund_detail:
                        fund_df.loc[idx, "前10大重仓股占比"] = fund_detail['前10大重仓股占比']
                    if '持股行业集中度' in fund_detail:
                        fund_df.loc[idx, "持股行业集中度"] = fund_detail['持股行业集中度']
            except Exception as e:
                print(f"基金代码{code}查询失败: {e}")

        # 前10大重仓股占比筛选：占比超过上限的基金剔除（数据缺失的保留，与既有筛选口径一致）
        def _extract_pct(value):
            m = re.search(r'([\d.]+)\s*%', str(value))
            return float(m.group(1)) if m else None

        before_count = len(fund_df)
        top10_pct = fund_df["前10大重仓股占比"].apply(_extract_pct)
        fund_df = fund_df[top10_pct.isna() | (top10_pct <= INDEX_FUND_MAX_TOP10_RATIO)].copy()
        removed = before_count - len(fund_df)
        print(f"{fund_type}基金: 按前10大重仓股占比不超过{INDEX_FUND_MAX_TOP10_RATIO}%筛选，"
              f"剔除 {removed} 只，保留 {len(fund_df)} 只")

        return fund_df.sort_values(by='近6月', ascending=False)
    except Exception as e:
        print(f"获取{fund_type}基金数据时出错: {e}")
        return pd.DataFrame()

def fetch_market_cap_fund_data(fund_type="小微盘", market_cap_threshold=25, market_cap_mode='lt', max_scale_yi=None, extra_filters=None):
    """按市值评分筛选基金数据（只保留各时间段前10名的基金）

    fund_type: 基金类型名称，如 '小微盘'、'大盘主动'
    market_cap_threshold: 市值评分阈值
    market_cap_mode: 'lt' 筛选市值评分 < 阈值，'gt' 筛选市值评分 > 阈值
    max_scale_yi: 最新规模上限（亿元），达到上限的基金剔除，None 表示不筛选规模
    extra_filters: 附加筛选条件字典，可含 min_excess_months（要求超额收益>0的时间段列表）、
                   max_top10_ratio（前10大重仓股占比上限%）、max_industry_concentration（持股行业集中度上限%）、
                   skip_top_period_filter（True 表示跳过"各时间段前10"筛选，保留全部候选）
    """
    compare_symbol = '>' if market_cap_mode == 'gt' else '<'
    try:
        # 从 jiuquaner_fund_style.py 获取符合市值评分条件的基金代码
        print(f'正在从韭圈儿获取市值评分 {compare_symbol} {market_cap_threshold} 的基金...')
        fund_list_df = get_fund_list(filter_types=['股票', '混合', '指数'])
        
        # 提前过滤债券和定开基金
        if 'name' in fund_list_df.columns:
            mask = ~fund_list_df['name'].str.contains('债券|定开|货币|人民币|美元|300|500|定期|纯债|A50|持有|黄金|A|H|R|ETF|指数|医药|医疗', na=False)
            fund_list_df = fund_list_df[mask].copy()
        
        fund_codes = fund_list_df['code'].tolist()
        
        # 创建基金代码->名称的映射
        fund_name_map = None
        if 'code' in fund_list_df.columns and 'name' in fund_list_df.columns:
            fund_name_map = dict(zip(fund_list_df['code'], fund_list_df['name']))
        
        # 批量获取基金风格数据
        result_df, filtered_df = batch_get_style(fund_codes, fund_name_map=fund_name_map, max_workers=20, filter_market_cap_threshold=market_cap_threshold, filter_market_cap_mode=market_cap_mode)
        
        if filtered_df is not None and not filtered_df.empty:
            matched_funds_df = pd.DataFrame({'code': filtered_df['基金代码'].tolist()})
            print(f'从韭圈儿获取到 {len(matched_funds_df)} 只市值评分 {compare_symbol} {market_cap_threshold} 的基金')
        else:
            print('未获取到符合条件的基金，使用全部获取到的基金')
            if not result_df.empty:
                matched_funds_df = pd.DataFrame({'code': result_df['基金代码'].tolist()})
            else:
                matched_funds_df = pd.DataFrame({'code': []})

        # 获取所有基金基础信息
        fund_open_fund_rank_em_df = ak.fund_open_fund_rank_em(symbol="全部")

        # 筛选出符合条件的基金
        fund_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["基金代码"].isin(matched_funds_df["code"])].copy()

        # 筛选出在各时间段任意一个进入前10的基金（含"近2年"，完整24个月动态窗口）
        # skip_top_period_filter=True 的类型（如大盘主动）不按各时间段前10筛选，保留全部候选
        if (extra_filters or {}).get("skip_top_period_filter"):
            print(f"{fund_type}基金: 不按各时间段前10筛选，保留全部 {len(fund_df)} 只候选基金")
        else:
            top_indices = select_top_indices_by_periods(fund_df)
            fund_df = fund_df.loc[list(top_indices)].copy()
            print(f"{fund_type}基金: 从全部基金中筛选出 {len(fund_df)} 只各时间段前10基金")

        # 附加筛选：超额收益曲线稳定性评分（参考 analyze_funds.py，基准为该类型配置的基准基金；评分无效的剔除）
        min_stability = (extra_filters or {}).get("min_stability_score")
        if min_stability is not None:
            benchmark_code = SPECIAL_FUND_BENCHMARKS.get(fund_type)
            benchmark_df = None
            if benchmark_code:
                print(f"{fund_type}基金: 正在获取基准基金 {benchmark_code} 净值用于稳定性评分...")
                for attempt in range(3):
                    benchmark_nav_df = get_fund_nav_history(benchmark_code, 365)
                    if benchmark_nav_df is not None and not benchmark_nav_df.empty:
                        break
                    print(f"获取基准基金 {benchmark_code} 净值失败(第{attempt+1}次)，2秒后重试...")
                    time.sleep(2)
                if benchmark_nav_df is not None and not benchmark_nav_df.empty:
                    # calculate_excess_return_curve 要求基准数据框含"指数"列
                    benchmark_df = benchmark_nav_df.rename(columns={'累计净值': '指数'})

            if benchmark_df is not None and not benchmark_df.empty:
                def fetch_stability_score(code):
                    try:
                        fund_nav_df = get_fund_nav_history(code, 365)
                        if fund_nav_df is None or fund_nav_df.empty:
                            return code, None
                        curve_data = calculate_excess_return_curve(fund_nav_df, benchmark_df)
                        if curve_data is None or len(curve_data) < 2:
                            return code, None
                        return code, calculate_stability_score(curve_data)
                    except Exception:
                        return code, None

                score_map = {}
                with ThreadPoolExecutor(max_workers=10) as score_executor:
                    score_futures = [score_executor.submit(fetch_stability_score, str(code)) for code in fund_df["基金代码"]]
                    for future in as_completed(score_futures):
                        code, score = future.result()
                        score_map[code] = score

                before_count = len(fund_df)
                fund_df["稳定性评分"] = fund_df["基金代码"].astype(str).map(score_map)
                fund_df = fund_df[fund_df["稳定性评分"].notna() & (fund_df["稳定性评分"] >= min_stability)].copy()
                print(f"{fund_type}基金: 按超额曲线稳定性评分 >= {min_stability} 筛选，从 {before_count} 只保留 {len(fund_df)} 只")
            else:
                print(f"{fund_type}基金: 未获取到基准基金({benchmark_code})净值，跳过稳定性评分筛选")

        if fund_df.empty:
            return pd.DataFrame()

        fund_df.loc[:, "成立时间"] = ""
        fund_df.loc[:, "最新规模"] = ""
        fund_df.loc[:, "换手率"] = ""
        fund_df.loc[:, "前10大重仓股占比"] = ""
        fund_df.loc[:, "持股行业集中度"] = ""

        def fetch_detail(code):
            """采集单只基金详情（供多线程调用）：规模/成立时间/换手率/前10大重仓/行业集中度。
            结果以字典返回，避免多线程并发写 DataFrame。"""
            result = {'最新规模': '', '成立时间': '', '换手率': '', '前10大重仓股占比': '', '持股行业集中度': ''}
            try:
                # 1. 根据基金代码查找基金名称 - 即使失败也继续
                fund_name = get_fund_name_by_code(str(code))
                if not fund_name:
                    print(f"无法获取基金 {code} 的名称，跳过该基金")
                    return result
                base_name = fund_name
                if fund_name.endswith('A') or fund_name.endswith('C'):
                    base_name = fund_name[:-1]
                # 2. 根据基金名称查找对应的基金代码(A+C)
                code_names = fetch_and_parse_fund_search(base_name)
                # 3. 获取A类和C类基金的规模数据
                if isinstance(code_names, list) and len(code_names) > 0:
                    for code_name in code_names:
                        info = get_fund_info(code_name['code'])
                    # 解析并累加规模数据
                    scale_match = re.search(r'([\d.]+)亿元', info['最新规模'])
                    if scale_match:
                        result['最新规模'] = f"{float(scale_match.group(1)):.2f}亿元"
                info = get_fund_info(code)
                result['成立时间'] = info.get('成立时间', '')
                # 获取换手率和重仓股信息
                fund_detail = parse_fund_data(code)
                if fund_detail:
                    result['换手率'] = fund_detail.get('换手率', '')
                    result['前10大重仓股占比'] = fund_detail.get('前10大重仓股占比', '')
                    result['持股行业集中度'] = fund_detail.get('持股行业集中度', '')
            except Exception as e:
                print(f"基金代码{code}查询失败: {e}")
            return result

        # 多线程采集各基金详情（如大盘主动候选达数百只，串行逐只抓取非常慢）
        detail_map = {}
        with ThreadPoolExecutor(max_workers=20) as detail_executor:
            futures = {detail_executor.submit(fetch_detail, str(row["基金代码"])): idx
                       for idx, row in fund_df.iterrows()}
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"获取{fund_type}基金详情"):
                idx = futures[future]
                try:
                    detail_map[idx] = future.result()
                except Exception as e:
                    print(f"基金详情采集失败: {e}")
        # 回填采集结果
        for idx, d in detail_map.items():
            fund_df.loc[idx, "最新规模"] = d['最新规模']
            fund_df.loc[idx, "成立时间"] = d['成立时间']
            fund_df.loc[idx, "换手率"] = d['换手率']
            fund_df.loc[idx, "前10大重仓股占比"] = d['前10大重仓股占比']
            fund_df.loc[idx, "持股行业集中度"] = d['持股行业集中度']

        # 按最新规模筛选：剔除规模达到上限的基金（无规模数据的保留）
        if max_scale_yi is not None:
            def extract_scale_yi(value):
                match = re.search(r'([\d.]+)亿元', str(value))
                return float(match.group(1)) if match else None

            before_count = len(fund_df)
            scale_yi = fund_df["最新规模"].apply(extract_scale_yi)
            fund_df = fund_df[scale_yi.isna() | (scale_yi < max_scale_yi)].copy()
            print(f"{fund_type}基金: 按最新规模 < {max_scale_yi}亿元筛选，从 {before_count} 只保留 {len(fund_df)} 只")

        # 附加筛选：前10大重仓股占比、持股行业集中度上限（数据缺失的保留）
        extra = extra_filters or {}
        max_top10 = extra.get("max_top10_ratio")
        max_industry = extra.get("max_industry_concentration")
        if max_top10 is not None or max_industry is not None:
            def extract_percent(value):
                match = re.search(r'([\d.]+)\s*%', str(value))
                return float(match.group(1)) if match else None

            before_count = len(fund_df)
            if max_top10 is not None and "前10大重仓股占比" in fund_df.columns:
                top10_pct = fund_df["前10大重仓股占比"].apply(extract_percent)
                fund_df = fund_df[top10_pct.isna() | (top10_pct < max_top10)].copy()
            if max_industry is not None and "持股行业集中度" in fund_df.columns:
                industry_pct = fund_df["持股行业集中度"].apply(extract_percent)
                fund_df = fund_df[industry_pct.isna() | (industry_pct < max_industry)].copy()
            conditions = []
            if max_top10 is not None:
                conditions.append(f"前10大重仓股占比 < {max_top10}%")
            if max_industry is not None:
                conditions.append(f"持股行业集中度 < {max_industry}%")
            print(f"{fund_type}基金: 按{'、'.join(conditions)}筛选，从 {before_count} 只保留 {len(fund_df)} 只")

        # 有稳定性评分时按评分倒序输出，否则按近6月倒序
        if "稳定性评分" in fund_df.columns:
            return fund_df.sort_values(by='稳定性评分', ascending=False, na_position='last')
        return fund_df.sort_values(by='近6月', ascending=False)
    except Exception as e:
        print(f"获取{fund_type}基金数据时出错: {e}")
        return pd.DataFrame()

def highlight_top_50_all_columns(df):
    """为表格数据添加样式标记"""
    # 创建一个样式DataFrame，默认为空字符串（无样式）
    styles = pd.DataFrame('', index=df.index, columns=df.columns)

    # 定义收益率列（去掉近1周；含"近2年"）
    return_columns = SCREEN_PERIOD_COLUMNS

    # 记录每个基金在多少个收益率列中进入前10
    top_count = {idx: 0 for idx in df.index}

    # 对每个收益率列，标记其前10名
    for col in return_columns:
        if col in df.columns:
            # 获取该列排序后的前10个索引（空值/非数值自动跳过）
            series = df[col]
            if series.dtype == object:
                series = pd.to_numeric(series, errors='coerce')
            top_10_idx = series.nlargest(10).index
            # 将对应位置设置为黄色背景
            styles.loc[top_10_idx, col] = 'background-color: yellow'
            # 更新每个基金进入前10的次数
            for idx in top_10_idx:
                top_count[idx] += 1

    # 对至少有3列进入前10的基金，将基金简称设置为金黄色背景
    for idx, count in top_count.items():
        if count >= 3:  # 至少有3列进入前10（共6个时间段）
            # 标注基金简称为金黄色
            styles.loc[idx, '基金简称'] = 'background-color: gold'

    return styles

def save_to_excel(writer, fund_df, sheet_name):
    """保存数据到Excel文件"""
    if fund_df is not None and not fund_df.empty:  # 确保DataFrame不为空
        styled_df = fund_df.style.apply(highlight_top_50_all_columns, axis=None)
        styled_df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        print(f"没有 {sheet_name} 数据可保存")

def highlight_excess_returns(df):
    """为超额收益率表格添加样式标记：正值标黄，至少3列正值的基金简称标金"""
    # 创建一个样式DataFrame，默认为空字符串（无样式）
    styles = pd.DataFrame('', index=df.index, columns=df.columns)

    # 定义参与标黄/标金的超额收益率列（排除近1周、近2年、近3年）
    excess_return_columns = ['近1月超额', '近3月超额', '近6月超额', '近1年超额', '今年来超额']

    # 记录每个基金有多少个超额收益率列为正值
    positive_count = {idx: 0 for idx in df.index}

    # 对每个超额收益率列，正值标黄
    for col in excess_return_columns:
        if col in df.columns:
            positive_idx = df[df[col] > 0].index
            styles.loc[positive_idx, col] = 'background-color: yellow'
            for idx in positive_idx:
                positive_count[idx] += 1

    # 对至少有3列为正值的基金，将基金简称设置为金黄色背景
    for idx, count in positive_count.items():
        if count >= 3:
            styles.loc[idx, '基金简称'] = 'background-color: gold'

    return styles

def _retry_call(func, max_retries=3, delay=2):
    """
    对网络调用进行重试，避免 IncompleteRead 等网络异常导致整体流程失败

    参数
    ------
    func : callable
        无参数的可调用对象（用 lambda 包装实际调用）
    max_retries : int
        最大尝试次数（含首次）
    delay : int
        每次重试前的等待秒数

    返回
    ------
    func 的返回值；若全部失败则抛出最后一次的异常
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_exc = e
            if attempt < max_retries - 1:
                print(f"网络调用失败(第{attempt+1}次): {e}，{delay}秒后重试...")
                time.sleep(delay)
    raise last_exc

def plot_excess_return_curves(fund_df, benchmark_code, fund_type, days=None, period_label='近2年'):
    """
    为基金绘制超额收益曲线图（参考 analyze_funds.py 的实现）
    只绘制"金黄色背景"的基金（至少3个超额收益率列为正值）

    参数
    ------
    fund_df : pd.DataFrame
        基金数据，需包含"基金代码"和"基金简称"列，以及超额收益率列
    benchmark_code : str
        基准基金代码（如 "510300"）
    fund_type : str
        基金类型名称（用于图标题和文件名）
    days : int, optional
        历史数据天数；默认 None 表示按当前系统日期前推完整24个自然月（近2年）
    period_label : str
        周期展示文案，默认"近2年"

    返回
    ------
    str or None
        生成的图片文件名，失败返回None
    """
    try:
        if fund_df is None or fund_df.empty:
            print(f"{fund_type}基金: 无数据可绘制")
            return None

        # 解析曲线展示区间：默认为当前系统日期往前完整24个自然月（近2年）
        if days is None:
            window_start, window_end = get_recent_two_year_window()
            days = (window_end - window_start).days
        else:
            window_end = date.today()
            window_start = window_end - timedelta(days=days)
            period_label = f'近{days}天'
        window_start_ts = pd.Timestamp(window_start)
        print(f"{fund_type}基金: 超额收益曲线展示区间 {period_label}："
              f"{window_start.strftime('%Y-%m-%d')} ~ {window_end.strftime('%Y-%m-%d')}（{days}天）")

        # 排除基准基金本身，避免在走势图和表格中显示
        if '基金代码' in fund_df.columns:
            before = len(fund_df)
            fund_df = fund_df[fund_df['基金代码'].astype(str).str.zfill(6) != str(benchmark_code).zfill(6)].copy()
            if len(fund_df) < before:
                print(f"{fund_type}基金: 排除基准基金 {benchmark_code}，剩余 {len(fund_df)} 只")

        # 筛选"金黄色背景"的基金：至少3个超额收益率列为正值
        excess_return_columns = ['近1月超额', '近3月超额', '近6月超额', '近1年超额', '今年来超额']
        available_cols = [col for col in excess_return_columns if col in fund_df.columns]
        
        if not available_cols:
            print(f"{fund_type}基金: 无超额收益率列，跳过筛选")
            selected_df = fund_df
        else:
            # 计算每只基金的正收益列数
            positive_counts = fund_df[available_cols].apply(
                lambda row: (row > 0).sum(), axis=1
            )
            # 保留至少3列为正值的基金
            mask = positive_counts >= 3
            selected_df = fund_df[mask].copy()
            print(f"{fund_type}基金: 从 {len(fund_df)} 只中筛选出 {len(selected_df)} 只正收益列数>=3的基金用于绘图")

            # 兜底：如果无基金满足金黄色条件，按"近6月超额"降序取前3只
            if selected_df.empty:
                print(f"{fund_type}基金: 无符合金黄色条件的基金，按'近6月超额'降序取前3只")
                sort_col = '近6月超额' if '近6月超额' in fund_df.columns else available_cols[0]
                selected_df = fund_df.sort_values(by=sort_col, ascending=False).head(3).copy()
                print(f"{fund_type}基金: 兜底选取 {len(selected_df)} 只基金用于绘图")

        if selected_df.empty:
            print(f"{fund_type}基金: 无符合条件的基金可绘制")
            return None

        # 获取基准基金净值历史（带重试，避免网络抖动导致整张图缺失）
        print(f"正在获取基准基金 {benchmark_code} 的净值历史...")
        benchmark_nav_df = None
        for attempt in range(3):
            benchmark_nav_df = get_fund_nav_history(benchmark_code, days)
            if benchmark_nav_df is not None and not benchmark_nav_df.empty:
                break
            print(f"获取基准基金 {benchmark_code} 净值失败(第{attempt+1}次)，2秒后重试...")
            time.sleep(2)
        if benchmark_nav_df is None or benchmark_nav_df.empty:
            print(f"无法获取基准基金 {benchmark_code} 的净值历史，跳过绘图")
            return None

        # calculate_excess_return_curve 要求基准数据框含"指数"列，这里把"累计净值"重命名
        benchmark_df = benchmark_nav_df.rename(columns={'累计净值': '指数'})

        # 获取基准基金简称，用于图表标题和图例显示
        benchmark_name = get_fund_name_by_code(str(benchmark_code)) or str(benchmark_code)
        display_label = f'{benchmark_name}({benchmark_code})'

        # 并行获取筛选后各基金的净值并计算超额收益曲线
        curves = []  # 元素: (fund_name, dates, excess_values)

        def fetch_curve(code, name):
            fund_nav_df = get_fund_nav_history(code, days)
            if fund_nav_df is None or fund_nav_df.empty:
                return None
            curve_data = calculate_excess_return_curve(fund_nav_df, benchmark_df)
            if curve_data is None or len(curve_data) == 0:
                return None
            # 将字符串日期转为 datetime 对象，确保 matplotlib 正确渲染 X 轴
            dates = [pd.to_datetime(item['date']) for item in curve_data]
            excess_values = [item['excess_return'] * 100 for item in curve_data]  # 转为百分比
            # 严格裁剪：只保留日期落在展示区间（近24个月）内的数据点
            in_window = [i for i, d in enumerate(dates) if d >= window_start_ts]
            dates = [dates[i] for i in in_window]
            excess_values = [excess_values[i] for i in in_window]
            if not dates:
                return None
            return (name, dates, excess_values)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for _, row in selected_df.iterrows():
                code = str(row["基金代码"])
                name = row.get("基金简称", code)
                futures.append(executor.submit(fetch_curve, code, name))

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result is not None:
                        curves.append(result)
                except Exception as e:
                    print(f"获取超额收益曲线失败: {e}")

        if not curves:
            print(f"{fund_type}基金: 未获取到任何超额收益曲线数据")
            return None

        # 只保留超额收益最高的3只基金（按曲线末端超额值排序）
        if len(curves) > 3:
            total_count = len(curves)
            curves.sort(key=lambda c: c[2][-1] if c[2] else float('-inf'), reverse=True)
            curves = curves[:3]
            print(f"{fund_type}基金: 从 {total_count} 只中保留超额收益最高的3只用于绘图")

        # 绘制图表
        n_curves = len(curves)
        # 动态调整画布大小，基金越多图越宽
        fig_width = max(14, 12 + n_curves * 0.8)
        fig_height = max(8, 6 + n_curves * 0.4)
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        # 使用差异化配色
        cmap = plt.cm.get_cmap('tab20', max(n_curves, 1))
        colors = [cmap(i) for i in range(n_curves)]

        # 绘制基金超额收益曲线
        for idx, (name, dates, excess_values) in enumerate(curves):
            ax.plot(dates, excess_values, label=name,
                    color=colors[idx],
                    linewidth=1.5,
                    alpha=0.85)
            # 在线末端直接标注基金名
            if len(dates) > 0:
                last_x = dates[-1]
                last_y = excess_values[-1]
                ax.annotate(
                    name,
                    xy=(last_x, last_y),
                    xytext=(5, 0),
                    textcoords='offset points',
                    fontsize=7,
                    va='center',
                    color=colors[idx],
                    annotation_clip=True
                )

        # 基准线：Y=0 的水平线（超额收益为0即基准收益），醒目绘制
        ax.axhline(y=0, color='black', linewidth=1.8, linestyle='--', alpha=0.85,
                   label=f'基准: {display_label}', zorder=10)
        # 在基准线右侧末端标注
        if curves and len(curves[0][1]) > 0:
            last_date = curves[0][1][-1]
            ax.annotate(
                f'基准: {display_label}',
                xy=(last_date, 0),
                xytext=(5, 8),
                textcoords='offset points',
                fontsize=8,
                va='bottom',
                color='black',
                fontweight='bold',
                annotation_clip=True
            )

        ax.set_title(
            f'{fund_type}基金超额收益曲线（基准：{display_label}，{period_label}：'
            f'{window_start.strftime("%Y-%m-%d")} 至 {window_end.strftime("%Y-%m-%d")}）',
            fontsize=14
        )
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('超额收益率 (%)', fontsize=12)
        ax.grid(True, alpha=0.3)

        # 设置合理的 x 轴刻度数量，避免日期标签过密
        import matplotlib.dates as mdates
        sample_dates = curves[0][1] if curves else []
        n_ticks = min(10, max(3, len(sample_dates) // 30 + 3)) if sample_dates else 6
        locator = mdates.AutoDateLocator(minticks=max(2, n_ticks - 2), maxticks=n_ticks + 2)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=8)

        # 将图例置于底部水平排列，紧凑样式（含基准线）
        legend = ax.legend(
            loc='upper center',
            bbox_to_anchor=(0.5, -0.15),
            ncol=min(4, n_curves + 1),
            fontsize=8,
            frameon=True,
            framealpha=0.8,
            edgecolor='#ccc'
        )
        legend.get_frame().set_linewidth(0.5)

        plt.tight_layout(rect=[0, 0.08, 1, 1])  # 底部留出图例空间

        img_filename = f'{fund_type}基金_超额曲线.png'
        fig.savefig(img_filename, dpi=150, bbox_inches='tight')
        plt.close(fig)

        # 保存曲线数据为JSON，供前端ECharts渲染
        # 每只基金携带自己的日期数据，支持不同成立时间的基金共存
        json_filename = f'{fund_type}基金_超额曲线.json'
        chart_data = {
            'fund_type': fund_type,
            'benchmark': display_label,
            'period': period_label,
            'start_date': window_start.strftime('%Y-%m-%d'),
            'end_date': window_end.strftime('%Y-%m-%d'),
            'series': [
                {
                    'name': c[0],
                    'dates': [d.strftime('%Y-%m-%d') for d in c[1]],
                    'data': [round(v, 4) for v in c[2]]
                } for c in curves
            ]
        }
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(chart_data, f, ensure_ascii=False, indent=2)

        print(f"{fund_type}基金超额收益曲线已保存为 {img_filename} 和 {json_filename}（共 {len(curves)} 只基金 + 基准）")
        return img_filename

    except Exception as e:
        print(f"绘制{fund_type}基金超额收益曲线时出错: {e}")
        import traceback
        traceback.print_exc()
        return None

def calculate_excess_returns(writer, selected_fund_types=None):
    """计算超额收益率并保存到Excel文件
    
    Args:
        writer: Excel写入器
        selected_fund_types: 需要处理的基金类型列表，为None时处理全部
    """
    try:
        # 定义基金类型与基准基金代码的映射关系
        benchmark_map = {
            "沪深300": "016690",
            "中证500": "009609",
            "A500":"022757",
            "中证800": "515810",
            "中证1000": "017645",
            "中证2000": "563300",
            "国证2000": "019319"
        }
        # 如果指定了基金类型，则只处理指定的
        if selected_fund_types is not None:
            benchmark_map = {k: v for k, v in benchmark_map.items() if k in selected_fund_types}
        # 定义收益率列
        return_columns = ['近1周', '近1月', '近3月', '近6月', '近1年', '近2年', '近3年', '今年来']
        # 获取基金排名数据（带重试，避免 IncompleteRead 等网络异常导致整体流程失败）
        fund_exchange_rank_em_df = _retry_call(lambda: ak.fund_exchange_rank_em())
        fund_open_fund_rank_em_df = _retry_call(lambda: ak.fund_open_fund_rank_em(symbol="混合型"))
        fund_open_fund_rank_stock_df = _retry_call(lambda: ak.fund_open_fund_rank_em(symbol="股票型"))
        fund_open_fund_rank_index_df = _retry_call(lambda: ak.fund_open_fund_rank_em(symbol="指数型"))
        # 为每种基金类型计算超额收益率
        for fund_type, benchmark_code in benchmark_map.items():
            print(f"正在处理{fund_type}基金的超额收益率，基准基金代码：{benchmark_code}")
            
            # 读取已保存的基金数据
            try:
                fund_df = pd.read_excel('index-fund.xlsx', sheet_name=f'{fund_type}基金')
            except:
                print(f"无法读取{fund_type}基金数据")
                continue
            
            if fund_df.empty:
                continue
            
            # 获取基准基金数据：按场内ETF -> 指数型 -> 股票型 -> 混合型 顺序查找
            benchmark_df = fund_exchange_rank_em_df[fund_exchange_rank_em_df["基金代码"] == benchmark_code]
            benchmark_source = "场内ETF(fund_exchange_rank_em)"

            if benchmark_df.empty:
                benchmark_df = fund_open_fund_rank_index_df[fund_open_fund_rank_index_df["基金代码"] == benchmark_code]
                benchmark_source = "指数型(fund_open_fund_rank_index)"

            if benchmark_df.empty:
                benchmark_df = fund_open_fund_rank_stock_df[fund_open_fund_rank_stock_df["基金代码"] == benchmark_code]
                benchmark_source = "股票型(fund_open_fund_rank_stock)"

            if benchmark_df.empty:
                benchmark_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["基金代码"] == benchmark_code]
                benchmark_source = "混合型(fund_open_fund_rank_em)"

            if benchmark_df.empty:
                print(f"未找到{fund_type}基金的基准基金{benchmark_code}")
                continue

            # 打印命中的基准基金信息，便于核对
            hit_code = benchmark_df["基金代码"].iloc[0]
            hit_name = benchmark_df["基金简称"].iloc[0] if "基金简称" in benchmark_df.columns else "<无基金简称列>"
            hit_date = benchmark_df["日期"].iloc[0] if "日期" in benchmark_df.columns else "<无日期列>"
            print(f"[基准核对] {fund_type} | 期望代码={benchmark_code} | 命中来源={benchmark_source} | 命中代码={hit_code} | 命中名称={hit_name} | 日期={hit_date}")

            # 打印各数据源中是否存在该代码，便于发现重复命中
            in_etf = (fund_exchange_rank_em_df["基金代码"] == benchmark_code).sum()
            in_index = (fund_open_fund_rank_index_df["基金代码"] == benchmark_code).sum()
            in_stock = (fund_open_fund_rank_stock_df["基金代码"] == benchmark_code).sum()
            in_mixed = (fund_open_fund_rank_em_df["基金代码"] == benchmark_code).sum()
            print(f"[命中分布] 代码 {benchmark_code} 在各数据源中的命中数量: 场内ETF={in_etf}, 指数型={in_index}, 股票型={in_stock}, 混合型={in_mixed}")

            # 获取基准收益率
            benchmark_returns = {}
            for col in return_columns:
                if col in benchmark_df.columns:
                    # 处理数值数据，确保正确转换
                    value = benchmark_df[col].iloc[0]
                    if pd.isna(value):
                        benchmark_returns[col] = 0
                    else:
                        # 如果是字符串，去除%符号并转换为数值
                        if isinstance(value, str):
                            benchmark_returns[col] = pd.to_numeric(value.rstrip('%'), errors='coerce') / 100
                        else:
                            # 如果已经是数值类型，直接使用
                            benchmark_returns[col] = pd.to_numeric(value, errors='coerce') / 100
                else:
                    benchmark_returns[col] = 0
            print(f"[基准收益率] {fund_type}({benchmark_code}): {benchmark_returns}")
            
            # 计算超额收益率（几何计算法，与走势图保持一致）
            for col in return_columns:
                if col in fund_df.columns:
                    # 创建超额收益率列名
                    excess_col = f'{col}超额'
                    # 将原始收益率转换为数值
                    fund_df[col] = pd.to_numeric(fund_df[col].astype(str).str.rstrip('%'), errors='coerce') / 100
                    # 几何超额收益率 = (1 + 基金收益) / (1 + 基准收益) - 1
                    fund_df[excess_col] = ((1 + fund_df[col]) / (1 + benchmark_returns[col]) - 1) * 100
            # 超额收益率列统一保留两位小数，确保Excel展示一致
            excess_cols = [c for c in fund_df.columns if c.endswith('超额')]
            if excess_cols:
                fund_df[excess_cols] = fund_df[excess_cols].round(2)

            # 只保留指定的列
            columns_to_keep = ["基金代码", "基金简称", "日期", "近1周超额", "近1月超额", "近3月超额", "近6月超额",
                              "近1年超额", "近2年超额", "近3年超额", "今年来超额", "成立时间", "最新规模", "市值评分", "换手率",
                               "前10大重仓股占比", "持股行业集中度"]
            # 检查哪些列实际存在于DataFrame中
            existing_columns = [col for col in columns_to_keep if col in fund_df.columns]
            fund_df = fund_df[existing_columns]
            
            # 确保基金代码为6位，不足的向前填充0
            if "基金代码" in fund_df.columns:
                fund_df["基金代码"] = fund_df["基金代码"].astype(str).str.zfill(6)
            
            # 保存带有超额收益率的数据，并应用样式
            styled_df = fund_df.style.apply(highlight_excess_returns, axis=None)
            styled_df.to_excel(writer, sheet_name=f'{fund_type}基金_超额', index=False)

            # 绘制超额收益曲线图（参考 analyze_funds.py）
            plot_excess_return_curves(fund_df, benchmark_code, fund_type)

        # 处理市值评分筛选类基金（小微盘、大盘主动等）的超额收益率
        for special_type, benchmark_code in SPECIAL_FUND_BENCHMARKS.items():
            # 根据配置决定是否处理该基金类型
            if selected_fund_types is not None and special_type not in selected_fund_types:
                continue
            try:
                special_fund_df = pd.read_excel('index-fund.xlsx', sheet_name=special_type)
                if special_fund_df.empty:
                    continue

                # 查找基准基金数据：按场内ETF -> 指数型 -> 股票型 -> 混合型 顺序查找
                benchmark_df = fund_exchange_rank_em_df[fund_exchange_rank_em_df["基金代码"] == benchmark_code]
                if benchmark_df.empty:
                    benchmark_df = fund_open_fund_rank_index_df[fund_open_fund_rank_index_df["基金代码"] == benchmark_code]
                if benchmark_df.empty:
                    benchmark_df = fund_open_fund_rank_stock_df[fund_open_fund_rank_stock_df["基金代码"] == benchmark_code]
                if benchmark_df.empty:
                    benchmark_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["基金代码"] == benchmark_code]

                if not benchmark_df.empty:
                    # 获取基准收益率
                    benchmark_returns = {}
                    for col in return_columns:
                        if col in benchmark_df.columns:
                            # 处理数值数据，确保正确转换
                            value = benchmark_df[col].iloc[0]
                            if pd.isna(value):
                                benchmark_returns[col] = 0
                            else:
                                # 如果是字符串，去除%符号并转换为数值
                                if isinstance(value, str):
                                    benchmark_returns[col] = pd.to_numeric(value.rstrip('%'), errors='coerce') / 100
                                else:
                                    # 如果已经是数值类型，直接使用
                                    benchmark_returns[col] = pd.to_numeric(value, errors='coerce') / 100
                        else:
                            benchmark_returns[col] = 0
                    
                    # 计算超额收益率（几何计算法，与走势图保持一致）
                    for col in return_columns:
                        if col in special_fund_df.columns:
                            # 创建超额收益率列名
                            excess_col = f'{col}超额'
                            # 将原始收益率转换为数值
                            special_fund_df[col] = pd.to_numeric(special_fund_df[col].astype(str).str.rstrip('%'), errors='coerce') / 100
                            # 几何超额收益率 = (1 + 基金收益) / (1 + 基准收益) - 1
                            special_fund_df[excess_col] = ((1 + special_fund_df[col]) / (1 + benchmark_returns[col]) - 1) * 100
                    # 超额收益率列统一保留两位小数，确保Excel展示一致
                    excess_cols = [c for c in special_fund_df.columns if c.endswith('超额')]
                    if excess_cols:
                        special_fund_df[excess_cols] = special_fund_df[excess_cols].round(2)

                    # 只保留指定的列
                    columns_to_keep = ["基金代码", "基金简称", "日期", "近1周超额", "近1月超额", "近3月超额", "近6月超额",
                                      "近1年超额", "近2年超额", "近3年超额", "今年来超额", "成立时间", "最新规模", "市值评分", "稳定性评分", "换手率",
                                       "前10大重仓股占比", "持股行业集中度"]
                    # 检查哪些列实际存在于DataFrame中
                    existing_columns = [col for col in columns_to_keep if col in special_fund_df.columns]
                    special_fund_df = special_fund_df[existing_columns]
                    
                    # 确保基金代码为6位，不足的向前填充0
                    if "基金代码" in special_fund_df.columns:
                        special_fund_df["基金代码"] = special_fund_df["基金代码"].astype(str).str.zfill(6)
                    
                    # 保存带有超额收益率的数据，并应用样式
                    styled_df = special_fund_df.style.apply(highlight_excess_returns, axis=None)
                    styled_df.to_excel(writer, sheet_name=f'{special_type}_超额', index=False)

                    # 绘制超额收益曲线图（参考 analyze_funds.py）
                    plot_excess_return_curves(special_fund_df, benchmark_code, special_type)
                else:
                    print(f"未找到{special_type}基金的基准基金{benchmark_code}")
            except Exception as e:
                print(f"处理{special_type}基金超额收益率时出错: {e}")
    except Exception as e:
        print(f"计算超额收益率时出错: {e}")

def adjust_column_width(filename):
    """调整Excel文件的列宽"""
    # 打开已存在的Excel文件
    workbook = load_workbook(filename)
    
    for sheet_name in workbook.sheetnames:
        worksheet = workbook[sheet_name]
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if cell.value and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # 限制最大宽度为50
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    workbook.save(filename)

def wait_file_available(filename, retry_interval=10):
    """等待文件可写。Windows 下文件被 Excel 打开时会加写锁，写入会抛 PermissionError，
    这里尝试以追加方式打开来检测占用，被占用则提示并等待，直到文件释放。"""
    while True:
        try:
            if os.path.exists(filename):
                with open(filename, 'ab'):
                    pass
            return
        except PermissionError:
            print(f"文件 {filename} 正被占用（可能已被 Excel 打开），请关闭 Excel，{retry_interval} 秒后自动重试...")
            time.sleep(retry_interval)

def update_fund_data():
    """更新基金数据的函数"""
    print(f"开始更新基金数据: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 先临时保存已筛选的基金数据（不包含市值评分）
        temp_data = {}
        filename = f'index-fund.xlsx'
        has_data = False

        # 自定义需要输出的基金类型，注释掉的不参与分析
        # 可选: "沪深300", "中证500", "A500", "中证800", "中证1000", "中证2000", "国证2000", "小微盘", "大盘主动"
        global SELECTED_FUND_TYPES
        SELECTED_FUND_TYPES = ["沪深300", "A500", "中证500", "中证1000", "国证2000", "小微盘", "大盘主动"]
        
        ALL_FUND_TYPES = ["沪深300", "中证500", "A500", "中证800", "中证1000", "中证2000", "国证2000"] + list(SPECIAL_MARKET_CAP_FILTERS.keys())
        # 指数基金类型与市值评分筛选类基金类型（小微盘、大盘主动等）分开处理
        fund_types = [t for t in SELECTED_FUND_TYPES if t in ALL_FUND_TYPES and t not in SPECIAL_MARKET_CAP_FILTERS]
        special_fund_types = [t for t in SELECTED_FUND_TYPES if t in SPECIAL_MARKET_CAP_FILTERS]
        all_fund_codes = []
        
        fund_type_names = "、".join(fund_types + special_fund_types)
        print(f"开始并行处理 {len(fund_types) + len(special_fund_types)} 种基金: {fund_type_names}...")
        
        def process_fund_type(fund_type):
            """处理单个基金类型的函数"""
            return fund_type, fetch_fund_data(fund_type)
        
        with ThreadPoolExecutor(max_workers=len(fund_types) + len(special_fund_types)) as executor:
            # 提交所有指数基金任务
            futures = {executor.submit(process_fund_type, ft): ft for ft in fund_types}
            # 提交市值评分筛选类基金任务（小微盘、大盘主动等）
            special_futures = {}
            for sf_type in special_fund_types:
                sf_threshold, sf_mode = SPECIAL_MARKET_CAP_FILTERS[sf_type]
                sf_max_scale = SPECIAL_FUND_MAX_SCALE.get(sf_type)
                sf_extra_filters = SPECIAL_FUND_EXTRA_FILTERS.get(sf_type)
                special_futures[executor.submit(fetch_market_cap_fund_data, sf_type, sf_threshold, sf_mode, sf_max_scale, sf_extra_filters)] = sf_type
            
            # 处理指数基金结果
            for future in as_completed(futures):
                fund_type = futures[future]
                try:
                    _, fund_df = future.result()
                    if fund_df is not None and not fund_df.empty:
                        temp_data[f'{fund_type}基金'] = fund_df
                        all_fund_codes.extend(fund_df['基金代码'].tolist())
                        has_data = True
                except Exception as e:
                    print(f"处理{fund_type}基金时出错: {e}")
            
            # 处理市值评分筛选类基金结果
            for future, sf_type in special_futures.items():
                try:
                    sf_df = future.result()
                    if sf_df is not None and not sf_df.empty:
                        temp_data[sf_type] = sf_df
                        all_fund_codes.extend(sf_df['基金代码'].tolist())
                        has_data = True
                except Exception as e:
                    print(f"处理{sf_type}基金时出错: {e}")
        
        # 收集所有需要市值评分的基金代码
        if all_fund_codes:
            # 去重
            all_fund_codes = list(set(all_fund_codes))
            print(f'筛选出 {len(all_fund_codes)} 只基金，正在获取它们的市值评分...')
            
            # 只针对这些基金获取市值评分
            fund_list_df = get_fund_list(filter_types=['股票', '混合', '指数'])
            fund_name_map = None
            if 'code' in fund_list_df.columns and 'name' in fund_list_df.columns:
                fund_name_map = dict(zip(fund_list_df['code'], fund_list_df['name']))
            
            # 批量获取市值评分
            result_df, filtered_df = batch_get_style(all_fund_codes, fund_name_map=fund_name_map, max_workers=20, filter_market_cap_threshold=None)
            
            # 创建基金代码到市值评分的映射
            market_cap_map = {}
            if result_df is not None and not result_df.empty:
                if '市值_本基金' in result_df.columns:
                    market_cap_map = dict(zip(result_df['基金代码'], result_df['市值_本基金']))
                print(f'获取到 {len(market_cap_map)} 只基金的市值评分')
            
            # 给每个基金DataFrame添加市值评分
            for sheet_name, fund_df in temp_data.items():
                fund_df.loc[:, "市值评分"] = ""
                for idx, row in fund_df.iterrows():
                    code = str(row["基金代码"])
                    if code in market_cap_map:
                        fund_df.loc[idx, "市值评分"] = market_cap_map[code]
                
                # 重新排列列顺序
                cols = list(fund_df.columns)
                if "最新规模" in cols and "市值评分" in cols:
                    latest_scale_idx = cols.index("最新规模")
                    if "市值评分" in cols:
                        cols.remove("市值评分")
                        cols.insert(latest_scale_idx + 1, "市值评分")
                    # 稳定性评分紧跟市值评分之后（仅市值评分筛选类基金有此列）
                    if "稳定性评分" in cols:
                        cols.remove("稳定性评分")
                        market_cap_idx = cols.index("市值评分")
                        cols.insert(market_cap_idx + 1, "稳定性评分")
                    temp_data[sheet_name] = fund_df[cols]
        
        # 现在保存到Excel（先确保文件未被 Excel 占用）
        wait_file_available(filename)
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, fund_df in temp_data.items():
                save_to_excel(writer, fund_df, sheet_name)
        
        # 只有当有数据时才继续处理
        if has_data:
            # 计算超额收益率并保存到新的工作表
            with pd.ExcelWriter(filename, engine='openpyxl', mode='a') as writer:
                calculate_excess_returns(writer, selected_fund_types=SELECTED_FUND_TYPES)
            
            # 调整列宽
            try:
                adjust_column_width(filename)
            except Exception as e:
                print(f"调整列宽时出错: {e}")
            
            print(f"已将所有C份额基金的排序结果保存为'{filename}'。")
        else:
            print("没有获取到任何基金数据")
        
        print(f"基金数据更新完成: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"更新基金数据时发生错误: {e}")
        import traceback
        traceback.print_exc()

def run_scheduler():
    """运行定时任务"""
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(f"调度器运行时发生错误: {e}")
        time.sleep(60)  # 每分钟检查一次


def start_scheduler():
    """启动定时任务"""
    # 立即执行一次更新
    update_fund_data()
    
    # 设置每天凌晨12:00执行更新
    schedule.every().day.at("00:00").do(update_fund_data)
    
    print("定时任务已启动，每天凌晨12:00将自动更新数据。")
    print("按 Ctrl+C 可以停止程序。")
    
    # 在单独的线程中运行定时任务
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # 保持主线程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序已被用户中断")
    except Exception as e:
        print(f"主线程发生错误: {e}")

if __name__ == '__main__':
    # 启动定时任务
    start_scheduler()