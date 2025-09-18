import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import json
import re
from datetime import datetime
import os

def is_today(date_str):
    """
    检查日期字符串是否是今天
    
    Args:
        date_str (str): 日期字符串，格式为 YYYY-MM-DD
    
    Returns:
        bool: 如果是今天返回True，否则返回False
    """
    if not date_str:
        return False
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return date.date() == datetime.now().date()
    except ValueError:
        return False

def load_cached_data(fund_data_file_path):
    """
    加载缓存的数据
    
    Returns:
        dict: 缓存的数据，如果不存在或不是今天的则返回None
    """
    if os.path.exists(fund_data_file_path):
        try:
            with open(fund_data_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查是否有更新时间且是今天
            if '_metadata' in data and 'update_time' in data['_metadata']:
                update_time = data['_metadata']['update_time']
                if is_today(update_time.split(' ')[0]):  # 只检查日期部分
                    print(f"使用缓存数据，更新时间: {update_time}")
                    return data
                else:
                    print(f"缓存数据不是今天的，需要重新获取。缓存更新时间: {update_time}")
            else:
                print("缓存数据中没有更新时间信息，需要重新获取")
        except Exception as e:
            print(f"读取缓存数据时出错: {e}")
    
    return None



def extract_fund_style_factors(fund_codes, fund_names=None,fund_data_file_path=None):
    """
    提取多只基金的风格因子数据
    
    Args:
        fund_codes (list): 基金代码列表
        fund_names (list, optional): 基金名称列表
        fund_data_file_path (str, optional): 基金数据文件路径
    
    Returns:
        dict: 包含所有基金风格因子数据的字典
    """
    # 首先尝试加载缓存数据
    cached_data = load_cached_data(fund_data_file_path)
    if cached_data:
        # 检查缓存中是否包含请求的基金代码
        funds_in_cache = {code: cached_data.get(code) for code in fund_codes if code in cached_data}
        # 如果所有请求的基金都在缓存中，则直接返回这些基金的数据
        if len(funds_in_cache) == len(fund_codes):
            print("所有请求的基金数据都已在缓存中，直接返回缓存数据")
            # 移除元数据部分再返回
            result = funds_in_cache.copy()
            return result
        elif len(funds_in_cache) > 0:
            print(f"部分基金数据已在缓存中 ({len(funds_in_cache)}/{len(fund_codes)})")
            # 可以选择只处理未缓存的基金，但当前实现将重新获取所有数据
        else:
            print("缓存中没有请求的基金数据")
    
    # 设置Chrome选项
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # 指定Chrome浏览器安装路径
    chrome_location = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
    chrome_options.binary_location = chrome_location
    # 指定ChromeDriver路径
    driver_path = r'C:\Users\18207\.wdm\drivers\chromedriver\win64\140.0.7339.82\chromedriver-win32\chromedriver.exe'
    # 创建Service对象
    service = Service(executable_path=driver_path)
    
    # 不使用无头模式，方便查看页面
    # chrome_options.add_argument("--headless")
    # 存储所有基金的数据
    all_funds_data = {}
    try:
        # 初始化WebDriver
        driver = webdriver.Chrome(service=service,options=chrome_options)
        driver.maximize_window()
        # 遍历每只基金代码
        for i, fund_code in enumerate(fund_codes):
            try:
                fund_data = {}
                
                # 添加基金名称到数据中
                if fund_names and i < len(fund_names):
                    fund_data["基金名称"] = fund_names[i]
                else:
                    fund_data["基金名称"] = f"基金({fund_code})"
                
                # 访问目标网页
                url = f"https://app.jiucaishuo.com/pagesA/gz/details?gu_code={fund_code}"
                print(f"正在访问: {url}")
                driver.get(url)
                # 等待页面加载
                time.sleep(3)
                # 点击"资产配置"按钮
                try:
                    # 等待并点击资产配置tab
                    allocation_tab = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[text()='资产配置']"))
                    )
                    driver.execute_script("arguments[0].click();", allocation_tab)
                    # print(f"基金 {fund_code}: 已点击'资产配置'按钮")
                    time.sleep(3)
                except Exception as e:
                    print(f"基金 {fund_code}: 点击'资产配置'按钮时出错: {e}")
                
                # 查找"持股风格"部分
                try:
                    # 等待并点击"持股风格"标签
                    style_tab = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), '持股风格')]"))
                    )
                    driver.execute_script("arguments[0].click();", style_tab)
                    print(f"基金 {fund_code}: 已点击'持股风格'标签")
                    time.sleep(3)
                except Exception as e:
                    print(f"基金 {fund_code}: 点击'持股风格'标签时出错: {e}")
                # 获取页面源码
                page_source = driver.page_source
                # 保存完整的页面源码以供进一步分析（仅对最后一只基金保存）
                if fund_code == fund_codes[0]:
                    with open('fund_full_page.html', 'w', encoding='utf-8') as f:
                        f.write(page_source)
                # 定义正则表达式模式来提取数据，不依赖于特定的data-v属性值
                pattern = r'<p data-v-[0-9a-f]{8}="" class="item">(.*?)<span data-v-[0-9a-f]{8}="" class="s1 jq_hm_font"[^>]*>(.*?)</span>/<span data-v-[0-9a-f]{8}="" class="s2 jq_hm_font"[^>]*>(.*?)</span></p>'
                # 查找所有匹配项
                matches = re.findall(pattern, page_source)
                # 提取数据
                style_factors = {}
                for match in matches:
                    factor_name = match[0].strip()
                    fund_value = float(match[1])
                    average_value = float(match[2])
                    style_factors[factor_name] = {
                        "基金值": fund_value,
                        "同类平均": average_value
                    }
                
                # 存储风格因子数据
                fund_data["风格因子"] = style_factors
                # print(f"基金 {fund_code}: 成功提取风格因子数据")
                
                # 点击"股票持仓"按钮并获取重仓股票前10占比
                try:
                    # 等待并点击"股票持仓"标签
                    stock_position_tab = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[text()='股票持仓']"))
                    )
                    driver.execute_script("arguments[0].click();", stock_position_tab)
                    # print(f"基金 {fund_code}: 已点击'股票持仓'按钮")
                    time.sleep(3)
                    
                    # 查找重仓股票前10占比信息
                    # 使用正则表达式从页面源码中提取"重仓股票(前10占比X.X%)"格式的信息
                    heavy_stock_pattern = r'<span data-v-[a-zA-Z0-9]+="" class="industry"[^>]*>(重仓股票\(前10占比\d+\.?\d*%\))</span>'
                    heavy_stock_matches = re.findall(heavy_stock_pattern, page_source)
                    
                    if heavy_stock_matches:
                        heavy_stock_info = heavy_stock_matches[0]  # 获取第一个匹配项
                        # 提取百分比数字
                        percentage_match = re.search(r'前10占比(\d+\.?\d*)%', heavy_stock_info)
                        if percentage_match:
                            top10_percentage = float(percentage_match.group(1))
                            fund_data["股票持仓"] = {
                                "重仓股票信息": heavy_stock_info,
                                "前十大重仓股占比": top10_percentage
                            }
                        else:
                            fund_data["股票持仓"] = {
                                "重仓股票信息": heavy_stock_info,
                                "前十大重仓股占比": None
                            }
                    else:
                        # 如果正则表达式未匹配到，尝试通过页面元素查找
                        industry_elements = driver.find_elements(By.XPATH, "//span[contains(@class, 'industry') and contains(text(), '重仓股票')]")
                        if industry_elements:
                            heavy_stock_info = industry_elements[0].text
                            # 提取百分比数字
                            percentage_match = re.search(r'前10占比(\d+\.?\d*)%', heavy_stock_info)
                            if percentage_match:
                                top10_percentage = float(percentage_match.group(1))
                                fund_data["股票持仓"] = {
                                    "重仓股票信息": heavy_stock_info,
                                    "前十大重仓股占比": top10_percentage
                                }
                            else:
                                fund_data["股票持仓"] = {
                                    "重仓股票信息": heavy_stock_info,
                                    "前十大重仓股占比": None
                                }
                        else:
                            fund_data["股票持仓"] = None
                            print(f"基金 {fund_code}: 未找到股票持仓数据")
                    
                except Exception as e:
                    print(f"基金 {fund_code}: 获取股票持仓数据时出错: {e}")
                    fund_data["股票持仓"] = None
                
                # 存储当前基金数据
                all_funds_data[fund_code] = fund_data
                
            except Exception as e:
                print(f"基金 {fund_code}: 提取数据时发生错误: {e}")
                all_funds_data[fund_code] = None
        
        # 保存结果到文件
        # 添加元数据信息
        all_funds_data['_metadata'] = {
            'update_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'fund_count': len(fund_codes)
        }
        with open(fund_data_file_path, 'w', encoding='utf-8') as f:
            json.dump(all_funds_data, f, ensure_ascii=False, indent=2)

        return all_funds_data
        
    except Exception as e:
        print(f"发生错误: {e}")
        return None
    finally:
        # 关闭浏览器
        if 'driver' in locals():
            driver.quit()

def find_similar_index(fund_data_file_path='fund_data.json', index_data_file_path='fund_style_factors.json'):
    """
    比较基金风格因子和指数风格因子，找出最接近的指数
    
    Args:
        fund_data_file_path (str): 基金数据文件路径
        index_data_file_path (str): 指数数据文件路径
    
    Returns:
        dict: 包含每个基金最接近指数信息的字典
    """
    # 读取基金数据
    with open(fund_data_file_path, 'r', encoding='utf-8') as f:
        fund_data = json.load(f)
    
    # 读取指数数据
    with open(index_data_file_path, 'r', encoding='utf-8') as f:
        index_data = json.load(f)
    
    # 移除元数据
    fund_data.pop('_metadata', None)
    index_data.pop('_metadata', None)
    
    # 创建结果字典
    result = {}
    
    # 为每个基金计算最接近的指数
    for fund_code, fund_info in fund_data.items():
        fund_name = fund_info.get("基金名称", f"基金({fund_code})")
        fund_style_factors = fund_info.get("风格因子", {})
        
        # 复制基金信息到结果中
        result[fund_code] = {
            "基金名称": fund_name,
            "风格因子": {}
        }
        
        # 为每个风格因子找到最接近的指数
        min_differences = {}  # 存储每个风格因子与各指数的差异
        
        # 初始化最小差异和最接近的指数
        for factor_name in fund_style_factors.keys():
            min_differences[factor_name] = {}
            for index_code, index_info in index_data.items():
                index_name = index_info.get("基金名称", f"指数({index_code})")
                index_style_factors = index_info.get("风格因子", {})
                
                if factor_name in index_style_factors:
                    fund_factor_value = fund_style_factors[factor_name]["基金值"]
                    index_factor_value = index_style_factors[factor_name]["基金值"]
                    
                    # 计算差异（绝对值）
                    difference = abs(fund_factor_value - index_factor_value)
                    min_differences[factor_name][index_code] = {
                        "指数名称": index_name,
                        "差异值": difference
                    }
        
        # 为每个风格因子找到差异最小的指数
        for factor_name, differences in min_differences.items():
            if differences:  # 确保有数据
                # 找到差异最小的指数
                closest_index_code = min(differences, key=lambda x: differences[x]["差异值"])
                closest_index_name = differences[closest_index_code]["指数名称"]
                
                # 构建风格因子信息
                result[fund_code]["风格因子"][factor_name] = {
                    "基金值": fund_style_factors[factor_name]["基金值"],
                    "同类平均": fund_style_factors[factor_name]["同类平均"],
                    "近似指数": closest_index_name
                }
    
    return result

if __name__ == "__main__":
    # 示例基金代码列表
    # fund_codes = ["510300","512510","516300","563300","159907"]  # 可以根据需要添加更多基金代码
    funds = [{"code": "510300", "name": "沪深300"}, {"code": "512510", "name": "中证500"},
             {"code": "516300", "name": "中证1000"}, {"code": "563300", "name": "中证2000"}, {"code": "159907", "name": "国证2000"}]
    fund_codes = [fund["code"] for fund in funds]
    fund_names = [fund["name"] for fund in funds]
    data = extract_fund_style_factors(fund_codes, fund_names)
    if data:
        print("成功提取基金风格因子数据")
        print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        print("未能提取基金风格因子数据")