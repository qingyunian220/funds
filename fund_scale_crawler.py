import requests
import pandas as pd
import json
import re


def crawl_fund_scale_data(fund_code):
    """
    爬取天天基金网基金规模变动详情数据
    
    Args:
        fund_code (str): 基金代码
        
    Returns:
        dict: 包含基金规模变动数据的字典
    """
    # 基金规模数据API
    url = f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    
    params = {
        "type": "gmbd",
        "mode": "0",
        "code": fund_code,
        "rt": "0.3293533905262933"
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Referer': f'http://fundf10.eastmoney.com/gmbd_{fund_code}.html'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.encoding = 'utf-8'
        
        # 解析返回的数据
        text = response.text
        
        # 使用正则表达式提取data字段中的JSON数据
        data_match = re.search(r'"data":\s*(\[[^\]]*\])', text)
        if not data_match:
            return {
                'fund_code': fund_code,
                'data': None,
                'status': 'failed',
                'message': '未能找到份额/净资产规模变动详情数据'
            }
        
        # 解析JSON数据
        data_str = data_match.group(1)
        data = json.loads(data_str)
        
        if data:
            # 转换数据格式以便于处理，只保留日期和期末净资产两个字段
            processed_data = [{
                '日期': item.get('FSRQ', ''),
                '期末净资产': round(item.get('NETNAV', 0) / 100000000, 2) if item.get('NETNAV') else None
            } for item in data]
            
            # 按日期排序并只保留前5条数据
            processed_data.sort(key=lambda x: x['日期'], reverse=True)
            processed_data = processed_data[:5]
            
            return {
                'fund_code': fund_code,
                'data': processed_data,
                'status': 'success'
            }
        
        return {
            'fund_code': fund_code,
            'data': None,
            'status': 'failed',
            'message': '未能找到份额/净资产规模变动详情数据'
        }
        
    except Exception as e:
        return {
            'fund_code': fund_code,
            'data': None,
            'status': 'error',
            'message': f'爬取数据时发生错误: {str(e)}'
        }

def save_to_json(data, filename):
    """
    将数据保存为JSON文件
    
    Args:
        data (dict): 要保存的数据
        filename (str): 文件名
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    """
    主函数，爬取诺安成长混合A(320007)的基金规模变动详情
    """
    fund_code = "320007"
    print(f"正在爬取基金 {fund_code} 的规模变动详情...")
    
    result = crawl_fund_scale_data(fund_code)
    
    if result['status'] == 'success':
        print("数据爬取成功!")
        print(f"共获取到 {len(result['data'])} 条记录")
        # 显示数据
        # for i, record in enumerate(result['data']):
        #     print(f"记录 {i+1}: {record}")
        return  result
        # 保存数据到JSON文件
        # save_to_json(result, f'fund_scale_data_{fund_code}.json')
        # print(f"数据已保存到 fund_scale_data_{fund_code}.json")
        
        # 保存为Excel文件
        # if result['data']:
        #     df = pd.DataFrame(result['data'])
        #     df.to_excel(f'fund_scale_data_{fund_code}.xlsx', index=False)
        #     print(f"数据已保存到 fund_scale_data_{fund_code}.xlsx")
    else:
        print(f"数据爬取失败: {result['message']}")


if __name__ == "__main__":
    main()