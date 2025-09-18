import pandas as pd
import requests
import json
import time
import re
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import gzip
import zlib
import brotli

# 基金规模：https://apiv2.jiucaishuo.com/funddetail/detail/fund-scale-change
# 资产分布：https://api.jiucaishuo.com/fundetail/fund-position/fundinvest

HEADER_JIUQUAN = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://apiv2.jiucaishuo.com/'
}

def create_session():
    """
    创建一个带有重试策略的会话
    """
    session = requests.Session()

    # 定义重试策略
    retry_strategy = Retry(
        total=3,  # 总重试次数
        backoff_factor=1,  # 重试间隔
        status_forcelist=[429, 500, 502, 503, 504],  # 需要重试的状态码
    )

    # 创建适配器并挂载到会话
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

def decompress_response_content(response):
    """
    尝试解压响应内容
    """
    content = response.content
    headers = response.headers

    # 检查Content-Encoding
    content_encoding = headers.get('Content-Encoding', '').lower()

    try:
        if 'gzip' in content_encoding:
            content = gzip.decompress(content)
        elif 'deflate' in content_encoding:
            content = zlib.decompress(content)
        elif 'br' in content_encoding:
            content = brotli.decompress(content)
    except Exception as e:
        print(f"解压响应内容失败: {str(e)}")
        return response.content
    return content

def extract_numeric_value(text):
    """
    从文本中提取数字和单位，例如从"基金最新一期规模5692.07万"提取"5692.07万"
    """
    if not isinstance(text, str):
        return text

    # 使用正则表达式匹配数字和单位（包括小数点、百分比符号、中文单位等）
    pattern = r'(\d+(?:\.\d+)?(?:[%万亿亿千万百十])*)'
    matches = re.findall(pattern, text)

    if matches:
        return matches[0]
    return text


def parse_fund_data(fund_code):
    """
    调用API接口并解析基金数据
    """
    url = "https://apiv2.jiucaishuo.com/funddetail/detail/fund-high-lights"
    payload = {
        "fund_code": fund_code,
        "type": "h5"
    }

    # 创建会话
    session = create_session()

    try:
        response = session.post(url, json=payload, headers=HEADER_JIUQUAN, timeout=15, stream=True)
        response.raise_for_status()

        # 尝试解压响应内容
        # content = decompress_response_content(response)
        content = response.content

        # 尝试不同的编码方式解码
        try:
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                text = content.decode('gbk')
            except UnicodeDecodeError:
                text = content.decode('utf-8', errors='ignore')

        # 清理文本内容
        text = text.strip()

        # 检查响应内容是否为空
        if not text:
            print(f"基金 {fund_code} 接口返回空内容")
            return None

        # 检查响应内容是否为有效的JSON
        try:
            # 尝试解析JSON之前，先检查内容是否看起来像JSON
            if not (text.startswith('{') or text.startswith('[')):
                print(f"基金 {fund_code} 接口返回非JSON内容: {text[:100]}...")
                return None

            data = json.loads(text)
        except json.JSONDecodeError as e:
            print(f"基金 {fund_code} JSON解析失败: {str(e)}")
            print(f"响应状态码: {response.status_code}")
            print(f"响应头: {dict(response.headers)}")
            print(f"响应内容前200字符: {text[:200]!r}")
            # 将响应内容保存到文件供分析
            with open(f'{fund_code}_response.txt', 'w', encoding='utf-8') as f:
                f.write(text)
            print(f"响应内容已保存到 {fund_code}_response.txt")
            return None

        # print("接口返回数据:", data)

        if data['code'] != 0:
            print(f"基金 {fund_code} 接口返回错误: {data['message']}")
            return None

        return parse_fund_details(data['data'], fund_code)

    except requests.exceptions.RequestException as e:
        print(f"基金 {fund_code} 请求失败: {str(e)}")
        return None
    except Exception as e:
        print(f"基金 {fund_code} 解析异常: {str(e)}")
        return None


def parse_fund_details(data, fund_code):
    """
    解析基金详细信息
    """
    result = {
        '基金代码': fund_code,
        '解析时间': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    # 解析持仓特征
    position_features = data.get('tssj_list', [])
    for feature in position_features:
        if feature.get('name') == '持仓特征':
            for tag in feature.get('tags', []):
                left_title = tag.get('left_title', '')
                info = tag.get('info', '')
                if '换手率' in left_title:
                    result['换手率'] = extract_numeric_value(info)

    return result['换手率']