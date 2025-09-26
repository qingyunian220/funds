import pandas as pd
import akshare as ak
import time
from tqdm import tqdm
import schedule
import threading
from flask import Flask, jsonify, request, render_template
import os
import json
import numpy as np

# Flask应用配置
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# 设置Excel文件路径
EXCEL_FILE = 'index-fund.xlsx'

def fetch_fund_data(fund_name):
    """获取指定类型基金数据"""
    # 获取所有基金基础信息
    fund_open_fund_rank_em_df = ak.fund_open_fund_rank_em(symbol="全部")
    
    fund_df = fund_open_fund_rank_em_df[fund_open_fund_rank_em_df["基金简称"].str.contains(fund_name, na=False)]
    fund_df = fund_df[fund_df["基金简称"].str.contains("C", na=False)]

    # 过滤掉"近3月"为空的数据
    fund_df = fund_df.dropna(subset=['近6月'])
    
    exclude_keywords = ["红利", "基本面", "价值", "非银", "成长", "低波动","信息技术","周期","非周期","地产"]
    for keyword in tqdm(exclude_keywords):
        fund_df = fund_df[~fund_df["基金简称"].str.contains(keyword, na=False)]
    
    fund_df["成立时间"] = ""
    fund_df["最新规模"] = ""
    
    for idx, row in tqdm(fund_df.iterrows(), total=fund_df.shape[0]):
        code = row["基金代码"]
        try:
            info = ak.fund_individual_basic_info_xq(symbol=code)
            if "成立时间" in info["item"].values:
                fund_df.at[idx, "成立时间"] = info.loc[info["item"] == "成立时间", "value"].values[0]
            if "最新规模" in info["item"].values:
                fund_df.at[idx, "最新规模"] = info.loc[info["item"] == "最新规模", "value"].values[0]
            time.sleep(0.1)
        except Exception as e:
            print(f"基金代码{code}查询失败: {e}")
    
    return fund_df.sort_values(by='近6月', ascending=False)

def highlight_top_50_all_columns(df):
    """为表格数据添加样式标记"""
    # 创建一个样式DataFrame，默认为空字符串（无样式）
    styles = pd.DataFrame('', index=df.index, columns=df.columns)
    
    # 定义收益率列
    return_columns = ['近1周','近1月', '近3月', '近6月', '近1年', '今年来']
    
    # 记录每个基金在多少个收益率列中进入前10
    top_count = {idx: 0 for idx in df.index}
    
    # 对每个收益率列，标记其前10名
    for col in return_columns:
        if col in df.columns:
            # 获取该列排序后的前10个索引
            top_10_idx = df[col].nlargest(10).index
            # 将对应位置设置为黄色背景
            styles.loc[top_10_idx, col] = 'background-color: yellow'
            # 更新每个基金进入前10的次数
            for idx in top_10_idx:
                top_count[idx] += 1
    
    # 对至少有5列进入前10的基金，将基金简称设置为金黄色背景
    for idx, count in top_count.items():
        if count >= 5:  # 至少有5列进入前10
            # 标注基金简称为金黄色
            styles.loc[idx, '基金简称'] = 'background-color: gold'
    
    return styles

def save_to_excel(writer, fund_df, sheet_name):
    """保存数据到Excel文件"""
    if not fund_df.empty:  # 确保DataFrame不为空
        styled_df = fund_df.style.apply(highlight_top_50_all_columns, axis=None)
        styled_df.to_excel(writer, sheet_name=sheet_name, index=False)

def update_fund_data():
    """更新基金数据的函数"""
    print(f"开始更新基金数据: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 创建一个ExcelWriter对象
    filename = f'index-fund.xlsx'
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        fund_types = ["沪深300", "中证500", "中证1000", "中证2000","国证2000"]
        for fund_type in fund_types:
            fund_df = fetch_fund_data(fund_type)
            save_to_excel(writer, fund_df, f'{fund_type}基金')

        # 自适应列宽
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column = [cell for cell in column]
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
    
    print(f"已将所有C份额基金的排序结果保存为'{filename}'，每个时间段的前10名标黄，至少有4个时间段进入前10的基金其简称标金黄色。")

def run_scheduler():
    """运行定时任务"""
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

# Flask路由
# @app.route('/')
# def index():
#     return render_template('fund_ranking.html')
#
# @app.route('/get_fund_data')
# def get_fund_data():
#     # 获取参数
#     sheet_name = request.args.get('sheet', '沪深300基金')
#     page = int(request.args.get('page', 1))
#     limit = int(request.args.get('limit', 20))
#
#     # 检查文件是否存在
#     if not os.path.exists(EXCEL_FILE):
#         return jsonify({'code': 1, 'msg': 'Excel文件不存在', 'count': 0, 'data': []})
#
#     try:
#         # 读取Excel文件中的指定工作表
#         df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)
#
#         # 处理数据
#         # 将所有列名中的空格去掉
#         df.columns = df.columns.str.replace(' ', '', regex=False)
#
#         # 处理 NaN 值，将其替换为 None（在 JSON 中会变成 null）
#         df = df.replace({np.nan: None})
#
#         # 处理日期格式，确保能正确序列化
#         date_columns = ['日期']
#         for col in date_columns:
#             if col in df.columns:
#                 df[col] = df[col].astype(str)
#
#         # 处理基金代码，确保为6位字符串格式
#         fund_code_columns = ['基金代码']
#         for col in fund_code_columns:
#             if col in df.columns:
#                 # 确保基金代码为字符串并格式化为6位
#                 df[col] = df[col].apply(lambda x: str(int(x)).zfill(6) if isinstance(x, (int, float)) and not pd.isna(x) else str(x).zfill(6) if x is not None else '')
#
#         # 分页处理
#         total = len(df)
#         start = (page - 1) * limit
#         end = start + limit
#         df_page = df.iloc[start:end]
#
#         # 转换为字典列表
#         data = df_page.to_dict('records')
#
#         # 手动构建 JSON 响应以确保中文正确显示
#         response_data = {
#             'code': 0,
#             'msg': 'success',
#             'count': total,
#             'data': data
#         }
#
#         # 使用 ensure_ascii=False 参数确保中文正确显示
#         response_json = json.dumps(response_data, ensure_ascii=False)
#
#         # 返回响应
#         return app.response_class(
#             response=response_json,
#             status=200,
#             mimetype='application/json'
#         )
#
#     except Exception as e:
#         response_data = {'code': 1, 'msg': str(e), 'count': 0, 'data': []}
#         response_json = json.dumps(response_data, ensure_ascii=False)
#         return app.response_class(
#             response=response_json,
#             status=200,
#             mimetype='application/json'
#         )

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

if __name__ == '__main__':
    # 启动定时任务
    start_scheduler()
    
    # 启动Flask应用
    app.run(debug=True, port=5000, use_reloader=False)