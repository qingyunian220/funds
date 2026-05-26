
from flask import Flask, render_template, jsonify, request
import json
import os
import pandas as pd
import numpy as np

app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')

# 尝试导入 analyze_funds 相关功能，如果失败则使用模拟数据
try:
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from analyze_funds import get_excess_return_curve_for_fund
    USE_REAL_DATA = True
    print('成功导入 analyze_funds，将使用真实数据')
except Exception as e:
    USE_REAL_DATA = False
    print(f'导入 analyze_funds 失败: {e}，将使用模拟数据')

@app.route('/')
def index():
    return render_template('quant_fund_ranking.html')

@app.route('/quant_fund_ranking')
def quant_fund_ranking():
    return render_template('quant_fund_ranking.html')


@app.route('/get_quant_fund_data')
def get_quant_fund_data():
    EXCEL_FILE = 'fund_open_fund_rank_em.xlsx'
    
    if not os.path.exists(EXCEL_FILE):
        return jsonify({'code': 1, 'msg': 'Excel文件不存在', 'count': 0, 'data': []})
    
    try:
        df = pd.read_excel(EXCEL_FILE)
        df = df.replace({np.nan: None})
        
        # 处理基金代码，确保为6位字符串格式
        if '基金代码' in df.columns:
            df['基金代码'] = df['基金代码'].apply(
                lambda x: str(int(x)).zfill(6) if isinstance(x, (int, float)) and not pd.isna(x) else str(x).zfill(6) if x is not None else ''
            )
        
        # 处理所有日期/时间类型，转换为字符串
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
            elif pd.api.types.is_timedelta64_dtype(df[col]):
                df[col] = df[col].astype(str)
        
        total = len(df)
        data = df.to_dict('records')
        
        response_data = {
            'code': 0,
            'msg': 'success',
            'count': total,
            'data': data
        }
        
        response_json = json.dumps(response_data, ensure_ascii=False)
        return app.response_class(
            response=response_json,
            status=200,
            mimetype='application/json'
        )
    
    except Exception as e:
        response_data = {'code': 1, 'msg': str(e), 'count': 0, 'data': []}
        response_json = json.dumps(response_data, ensure_ascii=False)
        return app.response_class(
            response=response_json,
            status=200,
            mimetype='application/json'
        )

@app.route('/get_excess_curve/<fund_code>')
def get_excess_curve(fund_code):
    print(f"\n=== 请求超额收益: {fund_code} ===")
    try:
        # 首先尝试从 Excel 文件中读取已有的数据
        EXCEL_FILE = 'fund_open_fund_rank_em.xlsx'
        if os.path.exists(EXCEL_FILE):
            try:
                df = pd.read_excel(EXCEL_FILE)
                # 处理基金代码格式，确保匹配
                if '基金代码' in df.columns:
                    df['基金代码'] = df['基金代码'].apply(
                        lambda x: str(int(x)).zfill(6) if isinstance(x, (int, float)) and not pd.isna(x) else str(x).zfill(6) if x is not None else ''
                    )
                    # 查找对应的基金
                    fund_row = df[df['基金代码'] == fund_code]
                    if len(fund_row) > 0 and '超额收益曲线' in df.columns:
                        curve_json = fund_row.iloc[0]['超额收益曲线']
                        if pd.notna(curve_json) and curve_json:
                            # 尝试解析 JSON
                            try:
                                if isinstance(curve_json, str):
                                    excess_curve = json.loads(curve_json)
                                else:
                                    excess_curve = curve_json
                                if excess_curve and isinstance(excess_curve, list) and len(excess_curve) > 0:
                                    print(f"从 Excel 成功读取数据: {len(excess_curve)} 个点")
                                    if len(excess_curve) > 0:
                                        first = excess_curve[0]
                                        last = excess_curve[-1]
                                        print(f"  起始: {first['date']} = {first['excess_return']*100:.2f}%")
                                        print(f"  结束: {last['date']} = {last['excess_return']*100:.2f}%")
                                    return jsonify({'code': 0, 'data': excess_curve})
                            except Exception as e:
                                print(f"解析 Excel 中的超额收益曲线失败: {e}")
            except Exception as e:
                print(f"从 Excel 读取失败: {e}")
        
        # Excel 中没有，才重新获取
        print("Excel 中无数据，重新获取...")
        if USE_REAL_DATA:
            excess_curve = get_excess_return_curve_for_fund(fund_code, days=365)
            if excess_curve and isinstance(excess_curve, list) and len(excess_curve) > 0:
                print(f"成功获取真实数据: {len(excess_curve)} 个点")
                if len(excess_curve) > 0:
                    first = excess_curve[0]
                    last = excess_curve[-1]
                    print(f"  起始: {first['date']} = {first['excess_return']*100:.2f}%")
                    print(f"  结束: {last['date']} = {last['excess_return']*100:.2f}%")
                return jsonify({'code': 0, 'data': excess_curve})
            else:
                print("真实数据为空，返回空数据")
                return jsonify({'code': 0, 'data': []})
        else:
            print("USE_REAL_DATA = False，返回空数据")
            return jsonify({'code': 0, 'data': []})
    except Exception as e:
        print(f'获取超额收益曲线失败: {e}')
        import traceback
        traceback.print_exc()
        # 出错时返回空数据
        return jsonify({'code': 0, 'data': []})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
