from complete_fund_style_extraction import extract_fund_style_factors, find_similar_index
import pandas as pd
import akshare as ak
import json
import schedule
import time
from datetime import datetime

from fund_data_processor import process_fund_data, process_fund_data_with_cache
from jiuquan_fund import parse_fund_data
from process_jiuquaner import process_jiuquaner_with_fund_names
from simuwang_browser_stable import simuwang


def run_fund_data_update():
    """
    运行基金数据更新任务
    """
    print(f"开始执行基金数据更新任务: {datetime.now()}")
    
    # 1.先获取几个宽基指数的数据
    # 定义基金代码和名称列表
    funds = [
        {"code": "510300", "name": "沪深300"},
        {"code": "512510", "name": "中证500"},
        {"code": "516300", "name": "中证1000"},
        {"code": "563300", "name": "中证2000"},
        {"code": "159907", "name": "国证2000"},
        {"code": "320016", "name": "小微盘"}

    ]
    fund_codes = [fund["code"] for fund in funds]
    fund_names = [fund["name"] for fund in funds]
    # 调用方法获取最新的基金数据
    data = extract_fund_style_factors(fund_codes, fund_names,'fund_style_factors.json')

    #2.获取基金名称
    excel_path = 'jiuquaner.xlsx'
    code_name_path = process_jiuquaner_with_fund_names(excel_path)
    #3.获取自选的基金数据
    # 读取Excel文件
    try:
        df = pd.read_excel(code_name_path, sheet_name=0, dtype={0: str})
        fund_codes = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
        fund_names = df.iloc[:, 1].dropna().astype(str).str.strip().tolist()
        fund_data_file_path = 'fund_data.json'
        # 提取风格因子数据并写入到json中
        data = extract_fund_style_factors(fund_codes, fund_names,fund_data_file_path)
        # 查找最接近的指数
        similar_index_data = find_similar_index(fund_data_file_path, 'fund_style_factors.json')
        # 将结果保存到fund_data.json文件中
        with open(fund_data_file_path, 'r', encoding='utf-8') as f:
            fund_data = json.load(f)
        # 更新基金数据，添加近似指数信息
        for fund_code, fund_info in similar_index_data.items():
            if fund_code in fund_data:
                fund_style_factors = fund_info.get("风格因子", {})
                for factor_name, factor_data in fund_style_factors.items():
                    if "近似指数" in factor_data and fund_code in fund_data and "风格因子" in fund_data[fund_code]:
                        if factor_name in fund_data[fund_code]["风格因子"]:
                            # 添加近似指数信息到基金数据中
                            fund_data[fund_code]["风格因子"][factor_name]["近似指数"] = factor_data["近似指数"]
        # 写入换手率数据
        for fund_code in fund_codes:
            fund_data[fund_code]['换手率']=parse_fund_data(fund_code)['换手率']
        # 写入更新后的数据到文件
        with open(fund_data_file_path, 'w', encoding='utf-8') as f:
            json.dump(fund_data, f, ensure_ascii=False, indent=2)
        
        # 添加基金规模信息
        print("\n正在获取基金规模信息...")
        for fund_code in fund_codes:
            print(f"{fund_code} at {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
            if fund_code in fund_data:
                print(f"正在获取基金 {fund_code} 的规模信息...")
                # 聚合规模信息和持有人结构信息并写入到json（带缓存机制）
                process_fund_data_with_cache(fund_code)
        
        print("\n已将基金风格因子、指数对比结果、规模信息和持有人结构信息更新到fund_data.json文件中")
        # 爬取私募排排网超额数据
        simuwang(fund_codes, fund_data_file_path)
        
        print(f"基金数据更新任务完成: {datetime.now()}")
    except Exception as e:
        print(f"读取Excel文件失败: {str(e)}")


def run_scheduler():
    """
    运行定时任务调度器
    """
    # 每天凌晨12点执行一次
    schedule.every().day.at("00:00").do(run_fund_data_update)
    
    # 程序启动时立即执行一次
    run_fund_data_update()
    
    # 持续运行调度器
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次


if __name__ == '__main__':
    run_scheduler()