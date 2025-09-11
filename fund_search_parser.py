import json
import re
import requests
from typing import Dict, List, Optional, Any


def parse_fund_search_response(response_text: str) -> list:
    """
    解析基金搜索API的响应数据
    
    Args:
        response_text (str): API返回的原始响应文本
        
    Returns:
        dict: 解析后的基金数据
    """
    try:
        # 首先尝试解析为JSON（直接JSON格式）
        data = json.loads(response_text)
    except json.JSONDecodeError:
        # 如果失败，尝试去除JSONP包装后再解析
        json_str = re.search(r'^[^(]*\((.*)\)$', response_text.strip())
        if not json_str:
            raise ValueError("无效的响应格式")
        data = json.loads(json_str.group(1))
    
    # 如果有错误码且不为0，返回错误信息
    if data.get("ErrCode", 0) != 0:
        return {
            "error": True,
            "error_code": data.get("ErrCode"),
            "error_message": data.get("ErrMsg", "未知错误")
        }
    
    # 解析基金数据
    parsed_funds = []
    for fund_data in data.get("Datas", []):
        parsed_fund = {
            # 基金基本信息
            "code": fund_data.get("CODE"),
            "name": fund_data.get("NAME"),
        }
        
        parsed_funds.append(parsed_fund)
    return parsed_funds



def fetch_and_parse_fund_search(keyword: str) -> List[Dict[str, str]]:
    """
    根据关键词获取并解析基金搜索数据
    
    Args:
        keyword (str): 搜索关键词
        
    Returns:
        list: 解析后的基金数据列表
    """
    url = "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx"
    
    params = {
        "m": "1",
        "key": keyword
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return parse_fund_search_response(response.text)
    except Exception as e:
        return {
            "error": True,
            "error_code": -1,
            "error_message": f"请求失败: {str(e)}"
        }



# 示例用法
if __name__ == "__main__":
    # 从网络获取并解析数据
    print("\n\n从网络获取'东方'关键词搜索结果:")
    network_data = fetch_and_parse_fund_search("东方兴瑞趋势领航混合")
    print(network_data)