from flask import Flask, jsonify
import akshare as ak
from complete_fund_style_extraction import extract_fund_style_factors
app = Flask(__name__)


@app.route('/api/fund_data/<code>')
def fund_data(code):
    """提供基金数据的API接口"""
    print("进来了")
    try:
        # 获取基金基本信息
        fund_individual_basic_info_xq_df = ak.fund_individual_basic_info_xq(symbol=code)
        print(fund_individual_basic_info_xq_df)

        if fund_individual_basic_info_xq_df.empty:
            return jsonify({"error": "未找到基金信息"}), 404

        # 正确提取基金名称
        fund_name_row = fund_individual_basic_info_xq_df[fund_individual_basic_info_xq_df['item'] == '基金名称']
        if not fund_name_row.empty:
            fund_name = fund_name_row['value'].iloc[0]
        else:
            fund_name = "未知"
        print(fund_name)

        # 获取基金风格因子数据
        data = extract_fund_style_factors([code], [fund_name])

        # 如果数据获取成功，返回数据
        if data:
            return jsonify(data)
        else:
            return jsonify({"error": "未能获取基金数据"}), 500

    except Exception as e:
        return jsonify({"error": f"获取基金数据时出错: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)