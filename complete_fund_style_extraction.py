import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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

def load_cached_data():
    """
    加载缓存的数据
    
    Returns:
        dict: 缓存的数据，如果不存在或不是今天的则返回None
    """
    if os.path.exists('fund_style_factors.json'):
        try:
            with open('fund_style_factors.json', 'r', encoding='utf-8') as f:
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

def extract_fund_style_factors(fund_codes, fund_names=None):
    """
    提取多只基金的风格因子数据
    
    Args:
        fund_codes (list): 基金代码列表
        fund_names (list, optional): 基金名称列表
    
    Returns:
        dict: 包含所有基金风格因子数据的字典
    """
    # 首先尝试加载缓存数据
    cached_data = load_cached_data()
    if cached_data:
        # 移除元数据部分再返回
        cached_data_copy = cached_data.copy()
        if '_metadata' in cached_data_copy:
            del cached_data_copy['_metadata']
        return cached_data_copy
    
    # 设置Chrome选项
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # 不使用无头模式，方便查看页面
    # chrome_options.add_argument("--headless")
    
    # 存储所有基金的数据
    all_funds_data = {}
    
    try:
        # 初始化WebDriver
        driver = webdriver.Chrome(options=chrome_options)
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
                    print(f"基金 {fund_code}: 已点击'资产配置'按钮")
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
                if fund_code == fund_codes[-1]:
                    with open('fund_full_page.html', 'w', encoding='utf-8') as f:
                        f.write(page_source)
                
                # 定义正则表达式模式来提取数据
                pattern = r'<p data-v-101724a4="" class="item">(.*?)<span data-v-101724a4="" class="s1 jq_hm_font"[^>]*>(.*?)</span>/<span data-v-101724a4="" class="s2 jq_hm_font"[^>]*>(.*?)</span></p>'
                
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
                print(f"基金 {fund_code}: 成功提取风格因子数据")
                
                # 点击"股票持仓"按钮并获取重仓股票前10占比
                try:
                    # 等待并点击"股票持仓"标签
                    stock_position_tab = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[text()='股票持仓']"))
                    )
                    driver.execute_script("arguments[0].click();", stock_position_tab)
                    print(f"基金 {fund_code}: 已点击'股票持仓'按钮")
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
                    
                    if fund_data["股票持仓"]:
                        print(f"基金 {fund_code}: 成功提取股票持仓数据: {fund_data['股票持仓']['重仓股票信息']}")
                    else:
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
        
        with open('fund_style_factors.json', 'w', encoding='utf-8') as f:
            json.dump(all_funds_data, f, ensure_ascii=False, indent=2)
        
        return all_funds_data
        
    except Exception as e:
        print(f"发生错误: {e}")
        return None
    finally:
        # 关闭浏览器
        if 'driver' in locals():
            driver.quit()

if __name__ == "__main__":
    # 示例基金代码列表
    # fund_codes = ["510300","512510","516300","563300","159907"]  # 可以根据需要添加更多基金代码
    funds = [{"code": "510300", "name": "华泰博瑞沪深300ETF"}, {"code": "512510", "name": "华泰柏瑞中证500ETF"},
             {"code": "516300", "name": "国泰柏瑞中证1000ETF"}, {"code": "563300", "name": "国泰柏瑞中证2000ETF"}, {"code": "159907", "name": "广发国证2000ETF"}]
    fund_codes = [fund["code"] for fund in funds]
    fund_names = [fund["name"] for fund in funds]
    data = extract_fund_style_factors(fund_codes, fund_names)
    if data:
        print("成功提取基金风格因子数据")
        print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        print("未能提取基金风格因子数据")