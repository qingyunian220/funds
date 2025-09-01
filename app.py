from flask import Flask, jsonify, render_template, send_from_directory
import json
import os
# 导入基金风格因子提取函数
from complete_fund_style_extraction import extract_fund_style_factors

app = Flask(__name__)

# 配置静态文件目录
STATIC_FOLDER = os.path.join(app.root_path, 'static')

@app.route('/')
def index():
    """提供前端页面"""
    return send_from_directory('.', 'fund_data_display.html')

@app.route('/api/fund_data')
def fund_data():
    """提供基金数据的API接口"""
    try:
        # 定义基金代码和名称列表
        funds = [
            {"code": "510300", "name": "华泰博瑞沪深300ETF"}, 
            {"code": "512510", "name": "华泰柏瑞中证500ETF"},
            {"code": "516300", "name": "国泰柏瑞中证1000ETF"}, 
            {"code": "563300", "name": "国泰柏瑞中证2000ETF"}, 
            {"code": "159907", "name": "广发国证2000ETF"}
        ]
        fund_codes = [fund["code"] for fund in funds]
        fund_names = [fund["name"] for fund in funds]
        
        # 调用方法获取最新的基金数据
        data = extract_fund_style_factors(fund_codes, fund_names)
        
        # 如果数据获取成功，返回数据
        if data:
            return jsonify(data)
        else:
            return jsonify({"error": "未能获取基金数据"}), 500
    except Exception as e:
        return jsonify({"error": f"获取基金数据时出错: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)