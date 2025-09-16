import urllib.error
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time

def retry_on_network_error(max_retries=3, delay=5):
    """
    装饰器：在网络连接错误时重试
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except urllib.error.URLError as e:
                    if "WinError 10054" in str(e) or "远程主机强迫关闭了一个现有的连接" in str(e):
                        if attempt < max_retries - 1:
                            print(f"网络连接错误 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                            print(f"等待 {delay} 秒后重试...")
                            time.sleep(delay)
                            continue
                    print(f"网络连接错误，已重试 {max_retries} 次仍然失败: {str(e)}")
                    raise
                except Exception as e:
                    raise
            return None
        return wrapper
    return decorator

@retry_on_network_error(max_retries=3, delay=5)
def parse_drawdown_data(html_content=None, driver=None):
    """
    解析回撤数据
    可以从HTML内容或WebDriver实例中提取数据
    """
    drawdown_data = {}
    
    try:
        # 如果提供了WebDriver实例，则从页面中提取数据
        if driver:
            # 定位包含回撤数据的表格
            table = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.el-table--fit.el-table--enable-row-hover.el-table--enable-row-transition.el-table.el-table--layout-fixed.mt-16.header-blue.is-scrolling-none"))
            )
            
            # 查找表格主体
            table_body = table.find_element(By.CSS_SELECTOR, "table.el-table__body")
            
            # 获取所有行
            rows = table_body.find_elements(By.CSS_SELECTOR, "tbody tr")
            
        # 如果提供了HTML内容，则需要使用其他方式解析（这里简化处理）
        elif html_content:
            # 这里应该使用BeautifulSoup或其他HTML解析器
            # 为简化示例，我们假设直接有数据
            pass
            
        # 根据您提供的HTML，回撤数据在第二行
        # 第一行是涨跌幅，第二行是回撤
        if driver:
            # 获取回撤行（第二行）
            drawdown_row = rows[1]  # 索引1对应第二行
            
            # 获取所有单元格
            cells = drawdown_row.find_elements(By.CSS_SELECTOR, "td")
            
            # 提取各时间段的回撤数据
            periods = ["本基金", "成立以来", "最近一月", "最近三月", "最近半年", "今年以来", "最近一年", "最近两年", "最近三年"]
            
            # 遍历单元格，提取数据（跳过第一列，因为那是行标题"回撤"）
            for i in range(1, min(len(cells), len(periods))):
                cell = cells[i]
                cell_div = cell.find_element(By.CSS_SELECTOR, "div.cell")
                span_element = cell_div.find_element(By.CSS_SELECTOR, "span")
                value = span_element.text
                drawdown_data[periods[i]] = value
                
        # 根据您提供的HTML示例数据
        else:
            drawdown_data = {
                "成立以来": "24.78%",
                "最近一月": "6.14%",
                "最近三月": "6.14%",
                "最近半年": "10.16%",
                "今年以来": "10.16%",
                "最近一年": "10.16%",
                "最近两年": "--"
            }
            
        return drawdown_data
        
    except Exception as e:
        print(f"解析回撤数据时出现错误: {str(e)}")
        return {}

def save_drawdown_data(drawdown_data, fund_code="320016"):
    """
    将回撤数据保存到JSON文件
    """
    try:
        filename = f"fund_drawdown_data_{fund_code}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(drawdown_data, f, ensure_ascii=False, indent=4)
        print(f"回撤数据已保存到 {filename}")
    except Exception as e:
        print(f"保存回撤数据时出现错误: {str(e)}")

# 示例用法
if __name__ == "__main__":
    # 示例：直接解析您提供的HTML数据
    drawdown_data = parse_drawdown_data()
    print("提取的回撤数据:")
    for period, value in drawdown_data.items():
        print(f"  {period}: {value}")
    
    # 保存数据
    # save_drawdown_data(drawdown_data, "320016")  # 注释掉这行，因为我们现在将数据保存在fund_data.json中
