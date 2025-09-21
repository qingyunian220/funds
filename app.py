from flask import Flask, render_template, jsonify
import json
import os

# 配置Flask应用，指定模板和静态文件目录
app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')

# 读取基金数据
def load_fund_data():
    with open(os.path.join(app.root_path, 'fund_data.json'), encoding='utf-8') as f:
        return json.load(f)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/fund')
def fund():
    return render_template('fund.html')

@app.route('/api/fund_data')
def fund_data():
    data = load_fund_data()
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)