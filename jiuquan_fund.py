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
        content = decompress_response_content(response)

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
                # 将响应内容保存到文件供分析
                # with open(f'{fund_code}_response.txt', 'w', encoding='utf-8') as f:
                #     f.write(text)
                # print(f"响应内容已保存到 {fund_code}_response.txt")
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


# def get_fund_top10_holdings(fund_code):
#     """
#     获取基金前十大重仓股的平均指标数据
#     """
#     # https://config.jiucaishuo.com/invest/001448_zz_category_none.json
#     url = f"https://config.jiucaishuo.com/invest/{fund_code}_zz_category_none.json"
#
#     # 创建会话
#     session = create_session()
#
#     try:
#         response = session.get(url, headers=HEADER_JIUQUAN, timeout=15, stream=True)
#         response.raise_for_status()
#
#         # 尝试解压响应内容
#         content = decompress_response_content(response)
#
#         # 尝试不同的编码方式解码
#         try:
#             text = content.decode('utf-8')
#         except UnicodeDecodeError:
#             try:
#                 text = content.decode('gbk')
#             except UnicodeDecodeError:
#                 text = content.decode('utf-8', errors='ignore')
#
#         # 清理文本内容
#         text = text.strip()
#
#         # 检查响应内容是否为空
#         if not text:
#             print(f"基金 {fund_code} 持仓接口返回空内容")
#             return None
#
#         # 检查响应内容是否为有效的JSON
#         try:
#             # 尝试解析JSON之前，先检查内容是否看起来像JSON
#             if not (text.startswith('{') or text.startswith('[')):
#                 print(f"基金 {fund_code} 持仓接口返回非JSON内容: {text[:100]}...")
#                 # 将响应内容保存到文件供分析
#                 with open(f'{fund_code}_holding_response.txt', 'w', encoding='utf-8') as f:
#                     f.write(text)
#                 print(f"响应内容已保存到 {fund_code}_holding_response.txt")
#                 return None
#
#             data = json.loads(text)
#         except json.JSONDecodeError as e:
#             print(f"基金 {fund_code} 持仓JSON解析失败: {str(e)}")
#             print(f"响应状态码: {response.status_code}")
#             print(f"响应头: {dict(response.headers)}")
#             print(f"响应内容前200字符: {text[:200]!r}")
#             # 将响应内容保存到文件供分析
#             with open(f'{fund_code}_holding_response.txt', 'w', encoding='utf-8') as f:
#                 f.write(text)
#             print(f"响应内容已保存到 {fund_code}_holding_response.txt")
#             return None
#
#         if data['code'] != 0:
#             print(f"基金 {fund_code} 持仓接口返回错误: {data['message']}")
#             return None
#
#         # 提取avg数据
#         avg_data = data.get('data', {}).get('gp', {}).get('avg', {})
#         if avg_data:
#             return {
#                 'PE': avg_data.get('pe_avg', ''),
#                 'PB': avg_data.get('pb_avg', ''),
#                 'ROE': avg_data.get('roe_avg', ''),
#                 '持股市值': avg_data.get('shizhi_avg', '')
#             }
#         return None
#
#     except requests.exceptions.RequestException as e:
#         print(f"基金 {fund_code} 持仓请求失败: {str(e)}")
#         return None
#     except Exception as e:
#         print(f"基金 {fund_code} 持仓解析异常: {str(e)}")
#         return None


# def get_fund_style_data(fund_code):
#     """
#     获取基金持股风格数据
#     """
#     url = "https://api.jiucaishuo.com/fundetail/fund-position/fundinvest"
#     payload = {
#         "fund_code": fund_code,
#         "tp": "cg",
#         "category": "zz_category"
#     }
#
#     # 创建会话
#     session = create_session()
#
#     try:
#         response = session.post(url, json=payload, headers=HEADER_JIUQUAN, timeout=15, stream=True)
#         response.raise_for_status()
#
#         # 尝试解压响应内容
#         content = decompress_response_content(response)
#
#         # 尝试不同的编码方式解码
#         try:
#             text = content.decode('utf-8')
#         except UnicodeDecodeError:
#             try:
#                 text = content.decode('gbk')
#             except UnicodeDecodeError:
#                 text = content.decode('utf-8', errors='ignore')
#
#         # 清理文本内容
#         text = text.strip()
#
#         # 检查响应内容是否为空
#         if not text:
#             print(f"基金 {fund_code} 风格接口返回空内容")
#             return None
#
#         # 检查响应内容是否为有效的JSON
#         try:
#             # 尝试解析JSON之前，先检查内容是否看起来像JSON
#             if not (text.startswith('{') or text.startswith('[')):
#                 print(f"基金 {fund_code} 风格接口返回非JSON内容: {text[:100]}...")
#                 # 将响应内容保存到文件供分析
#                 with open(f'{fund_code}_style_response.txt', 'w', encoding='utf-8') as f:
#                     f.write(text)
#                 print(f"响应内容已保存到 {fund_code}_style_response.txt")
#                 return None
#
#             data = json.loads(text)
#         except json.JSONDecodeError as e:
#             print(f"基金 {fund_code} 风格JSON解析失败: {str(e)}")
#             print(f"响应状态码: {response.status_code}")
#             print(f"响应头: {dict(response.headers)}")
#             print(f"响应内容前200字符: {text[:200]!r}")
#             # 将响应内容保存到文件供分析
#             with open(f'{fund_code}_style_response.txt', 'w', encoding='utf-8') as f:
#                 f.write(text)
#             print(f"响应内容已保存到 {fund_code}_style_response.txt")
#             return None
#
#         if data['code'] != 0:
#             print(f"基金 {fund_code} 风格接口返回错误: {data['message']}")
#             return None
#
#         # 提取infos数据
#         infos = data.get('data', {}).get('cg', {}).get('infos', [])
#         if infos:
#             style_data = {}
#             for info in infos:
#                 name = info.get('name', '')
#                 num1 = info.get('num1', '')
#                 num2 = info.get('num2', '')
#                 # 拼装成num1/num2格式
#                 style_data[name] = f"{num1}/{num2}"
#             return style_data
#         return None
#
#     except requests.exceptions.RequestException as e:
#         print(f"基金 {fund_code} 风格请求失败: {str(e)}")
#         return None
#     except Exception as e:
#         print(f"基金 {fund_code} 风格解析异常: {str(e)}")
#         return None


# def get_fund_style_distribution(fund_code):
#     """
#     获取基金风格分布数据
#     """
#     url = "https://api.jiucaishuo.com/fundetail/fund-position/fundinvest"
#     payload = {
#         "fund_code": fund_code,
#         "tp": "fg",
#         "category": "zz_category"
#     }
#
#     # 创建会话
#     session = create_session()
#
#     try:
#         response = session.post(url, json=payload, headers=HEADER_JIUQUAN, timeout=15, stream=True)
#         response.raise_for_status()
#
#         # 尝试解压响应内容
#         content = decompress_response_content(response)
#
#         # 尝试不同的编码方式解码
#         try:
#             text = content.decode('utf-8')
#         except UnicodeDecodeError:
#             try:
#                 text = content.decode('gbk')
#             except UnicodeDecodeError:
#                 text = content.decode('utf-8', errors='ignore')
#
#         # 清理文本内容
#         text = text.strip()
#
#         # 检查响应内容是否为空
#         if not text:
#             print(f"基金 {fund_code} 风格分布接口返回空内容")
#             return None
#
#         # 检查响应内容是否为有效的JSON
#         try:
#             # 尝试解析JSON之前，先检查内容是否看起来像JSON
#             if not (text.startswith('{') or text.startswith('[')):
#                 print(f"基金 {fund_code} 风格分布接口返回非JSON内容: {text[:100]}...")
#                 # 将响应内容保存到文件供分析
#                 with open(f'{fund_code}_style_dist_response.txt', 'w', encoding='utf-8') as f:
#                     f.write(text)
#                 print(f"响应内容已保存到 {fund_code}_style_dist_response.txt")
#                 return None
#
#             data = json.loads(text)
#         except json.JSONDecodeError as e:
#             print(f"基金 {fund_code} 风格分布JSON解析失败: {str(e)}")
#             print(f"响应状态码: {response.status_code}")
#             print(f"响应头: {dict(response.headers)}")
#             print(f"响应内容前200字符: {text[:200]!r}")
#             # 将响应内容保存到文件供分析
#             with open(f'{fund_code}_style_dist_response.txt', 'w', encoding='utf-8') as f:
#                 f.write(text)
#             print(f"响应内容已保存到 {fund_code}_style_dist_response.txt")
#             return None
#
#         if data['code'] != 0:
#             print(f"基金 {fund_code} 风格分布接口返回错误: {data['message']}")
#             return None
#
#         # 提取series数据
#         series = data.get('data', {}).get('fg', {}).get('series', [])
#         if series:
#             style_dist_data = {}
#             for item in series:
#                 name = item.get('name', '')
#                 data1 = item.get('data1', '')
#                 style_dist_data[name] = data1
#             return style_dist_data
#         return None
#
#     except requests.exceptions.RequestException as e:
#         print(f"基金 {fund_code} 风格分布请求失败: {str(e)}")
#         return None
#     except Exception as e:
#         print(f"基金 {fund_code} 风格分布解析异常: {str(e)}")
#         return None


# def get_fund_asset_allocation(fund_code):
#     """
#     获取基金资产配置数据
#     """
#     url = "https://api.jiucaishuo.com/fundetail/fund-position/fundinvest"
#     payload = {
#         "fund_code": fund_code,
#         "tp": "zc",
#         "category": "zz_category"
#     }
#
#     # 创建会话
#     session = create_session()
#
#     try:
#         response = session.post(url, json=payload, headers=HEADER_JIUQUAN, timeout=15, stream=True)
#         response.raise_for_status()
#
#         # 尝试解压响应内容
#         content = decompress_response_content(response)
#
#         # 尝试不同的编码方式解码
#         try:
#             text = content.decode('utf-8')
#         except UnicodeDecodeError:
#             try:
#                 text = content.decode('gbk')
#             except UnicodeDecodeError:
#                 text = content.decode('utf-8', errors='ignore')
#
#         # 清理文本内容
#         text = text.strip()
#
#         # 检查响应内容是否为空
#         if not text:
#             print(f"基金 {fund_code} 资产配置接口返回空内容")
#             return None
#
#         # 检查响应内容是否为有效的JSON
#         try:
#             # 尝试解析JSON之前，先检查内容是否看起来像JSON
#             if not (text.startswith('{') or text.startswith('[')):
#                 print(f"基金 {fund_code} 资产配置接口返回非JSON内容: {text[:100]}...")
#                 # 将响应内容保存到文件供分析
#                 with open(f'{fund_code}_asset_response.txt', 'w', encoding='utf-8') as f:
#                     f.write(text)
#                 print(f"响应内容已保存到 {fund_code}_asset_response.txt")
#                 return None
#
#             data = json.loads(text)
#         except json.JSONDecodeError as e:
#             print(f"基金 {fund_code} 资产配置JSON解析失败: {str(e)}")
#             print(f"响应状态码: {response.status_code}")
#             print(f"响应头: {dict(response.headers)}")
#             print(f"响应内容前200字符: {text[:200]!r}")
#             # 将响应内容保存到文件供分析
#             with open(f'{fund_code}_asset_response.txt', 'w', encoding='utf-8') as f:
#                 f.write(text)
#             print(f"响应内容已保存到 {fund_code}_asset_response.txt")
#             return None
#
#         if data['code'] != 0:
#             print(f"基金 {fund_code} 资产配置接口返回错误: {data['message']}")
#             return None
#
#         # 提取hy.series数据
#         series = data.get('data', {}).get('hy', {}).get('series', [])
#         if series:
#             # 按data值排序，取前三名
#             sorted_series = sorted(series, key=lambda x: x.get('data', 0), reverse=True)[:3]
#
#             asset_allocation_data = {}
#             for item in sorted_series:
#                 name = item.get('name', '')
#                 data_value = item.get('data', '')
#                 # 格式化为data%的形式
#                 asset_allocation_data[name] = f"{data_value}%"
#             return asset_allocation_data
#         return None
#
#     except requests.exceptions.RequestException as e:
#         print(f"基金 {fund_code} 资产配置请求失败: {str(e)}")
#         return None
#     except Exception as e:
#         print(f"基金 {fund_code} 资产配置解析异常: {str(e)}")
#         return None


# def get_fund_basic_info(fund_code):
#     """
#     获取基金基本信息（基金类型、业绩比较基准等）
#     """
#     url = "https://api.jiucaishuo.com/v2/fund-lists/fundsurvey"
#     payload = {
#         "code": fund_code
#     }
#
#     # 创建会话
#     session = create_session()
#
#     try:
#         response = session.post(url, json=payload, headers=HEADER_JIUQUAN, timeout=15, stream=True)
#         response.raise_for_status()
#
#         # 尝试解压响应内容
#         content = decompress_response_content(response)
#
#         # 尝试不同的编码方式解码
#         try:
#             text = content.decode('utf-8')
#         except UnicodeDecodeError:
#             try:
#                 text = content.decode('gbk')
#             except UnicodeDecodeError:
#                 text = content.decode('utf-8', errors='ignore')
#
#         # 清理文本内容
#         text = text.strip()
#
#         # 检查响应内容是否为空
#         if not text:
#             print(f"基金 {fund_code} 基本信息接口返回空内容")
#             return None
#
#         # 检查响应内容是否为有效的JSON
#         try:
#             # 尝试解析JSON之前，先检查内容是否看起来像JSON
#             if not (text.startswith('{') or text.startswith('[')):
#                 print(f"基金 {fund_code} 基本信息接口返回非JSON内容: {text[:100]}...")
#                 # 将响应内容保存到文件供分析
#                 with open(f'{fund_code}_basic_response.txt', 'w', encoding='utf-8') as f:
#                     f.write(text)
#                 print(f"响应内容已保存到 {fund_code}_basic_response.txt")
#                 return None
#
#             data = json.loads(text)
#         except json.JSONDecodeError as e:
#             print(f"基金 {fund_code} 基本信息JSON解析失败: {str(e)}")
#             print(f"响应状态码: {response.status_code}")
#             print(f"响应头: {dict(response.headers)}")
#             print(f"响应内容前200字符: {text[:200]!r}")
#             # 将响应内容保存到文件供分析
#             with open(f'{fund_code}_basic_response.txt', 'w', encoding='utf-8') as f:
#                 f.write(text)
#             print(f"响应内容已保存到 {fund_code}_basic_response.txt")
#             return None
#
#         if data['code'] != 0:
#             print(f"基金 {fund_code} 基本信息接口返回错误: {data['message']}")
#             return None
#
#         # 提取staus_list中的数据
#         staus_list = data.get('data', {}).get('staus_list', [])
#         basic_info = {}
#
#         for item in staus_list:
#             summary = item.get('summary', '')
#             value = item.get('value', '')
#
#             if summary == '基金类型':
#                 basic_info['基金类型'] = value
#             elif summary == '业绩比较基准':
#                 basic_info['业绩比较基准'] = value
#
#         return basic_info if basic_info else None
#
#     except requests.exceptions.RequestException as e:
#         print(f"基金 {fund_code} 基本信息请求失败: {str(e)}")
#         return None
#     except Exception as e:
#         print(f"基金 {fund_code} 基本信息解析异常: {str(e)}")
#         return None


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

                # if '股票仓位' in left_title:
                #     result['股票仓位'] = extract_numeric_value(info)
                # elif '换手率' in left_title:
                #     result['换手率'] = extract_numeric_value(info)
                # elif '持股集中度' in left_title:
                #     result['前10大重仓股占比'] = extract_numeric_value(info)
                # elif '持股行业集中度' in left_title:
                #     result['持股行业集中度'] = info  # 保持原样
                # elif '含AI量' in left_title:
                #     result['含AI量'] = extract_numeric_value(info)
                # elif '港股仓位' in left_title:
                #     result['港股仓位'] = info  # 保持原样

    # 解析其他特征 - 基金规模
    # other_features = data.get('tssj_list', [])
    # for feature in other_features:
    #     if feature.get('name') == '其他特征':
    #         for tag in feature.get('tags', []):
    #             if '基金规模' in tag.get('left_title', ''):
    #                 result['基金规模'] = extract_numeric_value(tag.get('info', ''))

    # 解析基金经理信息
    # manager_features = data.get('tssj_list', [])
    # for feature in manager_features:
    #     if feature.get('name') == '基金经理':
    #         for tag in feature.get('tags', []):
    #             left_title = tag.get('left_title', '')
    #             info = tag.get('info', '')
    #             if '经理得分' in left_title:
    #                 result['经理得分'] = info
    #             elif '管理总规模' in left_title:
    #                 result['管理总规模'] = info
    #             elif '投资年限' in left_title:
    #                 result['投资年限'] = info

    # 解析进攻水平
    # offense_features = data.get('tssj_list', [])
    # for feature in offense_features:
    #     if feature.get('name') == '进攻力':
    #         for tag in feature.get('tags', []):
    #             left_title = tag.get('left_title', '')
    #             info = tag.get('info', '')
    #
    #             if '超额收益率' in left_title:
    #                 result['超额收益率'] = info
    #             elif '卡玛比率' in left_title:
    #                 result['卡玛比率'] = info

    # 获取前十大重仓股的平均指标
    # top10_avg = get_fund_top10_holdings(fund_code)
    # if top10_avg:
    #     result.update(top10_avg)
    #
    # # 获取基金持股风格数据
    # style_data = get_fund_style_data(fund_code)
    # if style_data:
    #     result.update(style_data)
    #
    # # 获取基金风格分布数据
    # style_dist_data = get_fund_style_distribution(fund_code)
    # if style_dist_data:
    #     result.update(style_dist_data)
    #
    # # 获取基金资产配置数据
    # asset_allocation_data = get_fund_asset_allocation(fund_code)
    # if asset_allocation_data:
    #     result.update(asset_allocation_data)
    #
    # # 获取基金基本信息（基金类型、业绩比较基准）
    # basic_info = get_fund_basic_info(fund_code)
    # if basic_info:
    #     result.update(basic_info)

    return result['换手率']


# def fetch_from_jiuquan(excel_path, output_path=None):
#     """
#     处理Excel中的基金代码并调用API
#     """
#     # 读取Excel文件
#     try:
#         df = pd.read_excel(excel_path, sheet_name=0, dtype={0: str})
#         fund_codes = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
#
#         if not fund_codes:
#             print("Excel文件中没有找到基金代码")
#             return False
#
#         print(f"找到 {len(fund_codes)} 个基金代码: {fund_codes}")
#
#     except Exception as e:
#         print(f"读取Excel文件失败: {str(e)}")
#         return False
#
#     # 处理每个基金代码
#     all_fund_data = []
#
#     for i, fund_code in enumerate(fund_codes, 1):
#         print(f"处理第 {i}/{len(fund_codes)} 个基金: {fund_code}")
#
#         fund_data = parse_fund_data(fund_code)
#         if fund_data:
#             all_fund_data.append(fund_data)
#
#         # 添加延迟，避免请求过于频繁
#         time.sleep(0.5)
#
#     if not all_fund_data:
#         print("没有成功获取到任何基金数据")
#         return False
#
#     # 创建DataFrame
#     result_df = pd.DataFrame(all_fund_data)
#
#     # 定义列的顺序和分组
#     column_groups = {
#         '基本信息': ['基金代码', '基金规模', '基金类型', '业绩比较基准'],
#         '持仓特征': ['前10大重仓股占比', '港股仓位', '股票仓位', '持股行业集中度', '含AI量', '换手率'],
#         '进攻能力': ['卡玛比率', '超额收益率'],
#         '基金经理': ['投资年限', '管理总规模', '经理得分'],
#         '前10重仓股票': ['PE', 'PB', 'ROE', '持股市值'],  # 新增的分组
#         '持股风格': ['市值', '成长', '盈利', '价值'],  # 持股风格分组
#         '基金风格': ['小盘成长', '大盘成长'],  # 基金风格分组
#         '资产配置': ['债券', '股票', '现金']  # 资产配置分组（根据实际数据可能需要调整）
#     }
#
#     # 构建完整的列列表
#     all_columns = []
#     for group_cols in column_groups.values():
#         all_columns.extend(group_cols)
#
#     # 确保所有列都存在于DataFrame中，不存在的列用空值填充
#     for col in all_columns:
#         if col not in result_df.columns:
#             result_df[col] = ''
#
#     # 重新排列列的顺序
#     result_df = result_df[all_columns]
#
#     # 确定输出文件路径
#     if output_path is None:
#         output_path = excel_path.replace('.xlsx', '_processed.xlsx')
#
#     # 保存到Excel文件，使用两级表头并合并相同表头，添加样式
#     try:
#         # 创建Excel写入器
#         with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
#             # 先将数据写入Excel（不带多级表头）
#             result_df.to_excel(writer, sheet_name='基金数据汇总', index=False, startrow=1)
#
#             # 获取工作表
#             worksheet = writer.sheets['基金数据汇总']
#
#             # 手动写入表头并合并相同名称的单元格
#             headers = []
#             sub_headers = []
#             for group_name, group_cols in column_groups.items():
#                 headers.extend([group_name] * len(group_cols))
#                 sub_headers.extend(group_cols)
#
#             # 写入第一行表头并合并相同名称的单元格
#             col_idx = 1  # Excel列索引从1开始
#             merged_cells_info = []  # 记录合并的单元格信息
#             for group_name, group_cols in column_groups.items():
#                 # 写入组名
#                 start_col = col_idx
#                 end_col = col_idx + len(group_cols) - 1
#
#                 # 写入组名到第一行并合并单元格
#                 cell = worksheet.cell(row=1, column=start_col, value=group_name)
#                 if len(group_cols) > 1:
#                     worksheet.merge_cells(start_row=1, start_column=start_col,
#                                           end_row=1, end_column=end_col)
#                     merged_cells_info.append((1, start_col, 1, end_col))
#                 else:
#                     merged_cells_info.append((1, start_col, 1, start_col))
#
#                 col_idx += len(group_cols)
#
#             # 写入第二行表头（具体字段名）
#             for col_idx, sub_header in enumerate(sub_headers, 1):
#                 worksheet.cell(row=2, column=col_idx, value=sub_header)
#
#             # 添加样式
#             from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
#
#             # 创建样式
#             # 第一行表头样式：绿色背景、加粗、居中
#             header_font = Font(bold=True)
#             header_fill = PatternFill(start_color="90EE90", end_color="90EE90", fill_type="solid")
#             center_alignment = Alignment(horizontal="center", vertical="center")
#
#             # 边框样式
#             thin_border = Border(
#                 left=Side(style='thin'),
#                 right=Side(style='thin'),
#                 top=Side(style='thin'),
#                 bottom=Side(style='thin')
#             )
#
#             # 应用第一行表头样式
#             for start_row, start_col, end_row, end_col in merged_cells_info:
#                 # 对于合并的单元格，只需要设置左上角的单元格
#                 cell = worksheet.cell(row=start_row, column=start_col)
#                 cell.font = header_font
#                 cell.fill = header_fill
#                 cell.alignment = center_alignment
#                 cell.border = thin_border
#
#             # 应用第二行表头样式：居中+边框
#             for col_idx in range(1, len(sub_headers) + 1):
#                 cell = worksheet.cell(row=2, column=col_idx)
#                 cell.alignment = center_alignment
#                 cell.border = thin_border
#
#             # 设置所有数据单元格居中+边框
#             for row in range(3, len(result_df) + 3):
#                 for col in range(1, len(result_df.columns) + 1):
#                     cell = worksheet.cell(row=row, column=col)
#                     cell.alignment = center_alignment
#                     cell.border = thin_border
#
#         print(f"数据已保存到: {output_path}")
#         return True
#
#     except Exception as e:
#         print(f"保存Excel文件失败: {str(e)}")
#         return False