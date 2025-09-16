import random
import urllib.error

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager
from webdriver_manager.core.http import HttpClient
from selenium.webdriver.common.keys import Keys
import time
import os
import sys
import json
import undetected_chromedriver as uc

from parse_drawdown_data import parse_drawdown_data


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
                    # 对于非网络错误，直接抛出
                    raise
            return None

        return wrapper

    return decorator


class SimuwangBrowser:
    def __init__(self):
        self.driver = None
        self.config = self.load_config()

    def load_config(self):
        """
        加载配置文件
        """
        try:
            with open('config.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print("未找到配置文件 config.json，请创建该文件并添加手机号和密码信息")
            return {}
        except Exception as e:
            print(f"读取配置文件时出现错误: {str(e)}")
            return {}

    @retry_on_network_error(max_retries=3, delay=5)
    def open_simuwang(self):
        """
        打开私募排排网公募基金页面并等待登录弹窗
        """
        try:
            print("正在初始化浏览器...")
            self.driver = uc.Chrome(
                options=uc.ChromeOptions(),
                browser_executable_path=r'C:\Program Files\Google\Chrome\Application\chrome.exe',
                driver_executable_path=r'C:\Users\18207\.wdm\drivers\chromedriver\win64\140.0.7339.82\chromedriver-win32\chromedriver.exe',
                version_main=140  # 匹配你的Chrome版本
            )
            # 设置页面加载超时
            self.driver.set_page_load_timeout(60)  # 增加超时时间到60秒

            # 直接打开公募基金页面
            print("正在打开私募排排网公募基金页面...")
            self.driver.get("https://www.simuwang.com/gmjj")

            # 等待页面加载完成
            time.sleep(3)

            # 等待登录弹窗出现
            print("等待登录弹窗出现...")
            return True

        except Exception as e:
            print(f"打开私募排排网时出现错误: {str(e)}")
            if self.driver:
                self.driver.quit()
            return False

    def login(self):
        """
        登录操作
        """
        try:
            # 使用指定的选择器定位登录弹窗
            login_popup = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-v-ba0c5dd9].w-fit"))
            )
            print("找到登录弹窗")
            time.sleep(random.uniform(1, 3))

            # 点击密码登录按钮
            password_login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.nav-btn.ml-24"))
            )
            password_login_button.click()
            print("已点击密码登录按钮")
            time.sleep(random.uniform(1, 3))

            # 输入手机号和密码
            # 等待手机号输入框出现并可交互
            phone_input = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[autocomplete='username']"))
            )
            # 点击手机号输入框确保其获得焦点
            phone_input.click()
            # 清空并输入手机号
            phone_input.clear()
            phone_input.send_keys(self.config.get("phone", ""))  # 从配置文件读取手机号
            print("已输入手机号")
            time.sleep(random.uniform(1, 3))

            # 等待密码输入框出现并可交互
            password_input = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='password']"))
            )
            # 点击密码输入框确保其获得焦点
            password_input.click()
            # 清空并输入密码
            password_input.clear()
            password_input.send_keys(self.config.get("password", ""))  # 从配置文件读取密码
            print("已输入密码")
            time.sleep(random.uniform(1, 3))

            # 点击复选框
            try:
                # 如果通过input无法点击，则尝试点击其父元素span
                checkbox_span = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "span.el-checkbox__inner"))
                )
                checkbox_span.click()
                print("已通过span点击复选框")
                time.sleep(random.uniform(1, 3))
            except Exception as e2:
                print(f"通过span点击复选框时出现错误: {str(e2)}")

            # 点击登录按钮
            login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.el-button.login-btn"))
            )
            login_button.click()
            print("已点击登录按钮")
            time.sleep(random.uniform(1, 3))

            return True

        except Exception as e:
            print(f"登录过程中出现错误: {str(e)}")
            return False

    def search_fund(self, fund_code):
        """
        搜索基金
        """
        try:
            # 每次搜索前都回到基金首页
            print("正在返回基金首页...")
            self.driver.get("https://www.simuwang.com/gmjj")
            time.sleep(3)

            # 等待搜索框出现
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "nav_keyword"))
            )
            # 清空搜索框
            search_input.clear()
            # 输入基金代码
            search_input.send_keys(fund_code)
            # 按回车键搜索
            search_input.send_keys(Keys.RETURN)
            print(f"正在搜索基金代码：{fund_code}")
            # 等待搜索结果加载
            time.sleep(3)
            # 尝试多种方式定位和点击搜索结果链接
            result_link = None
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    print(f"尝试第 {attempt + 1} 次点击搜索结果链接...")
                    # 方法1: 使用原始的CSS选择器
                    try:
                        result_link = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable(
                                (By.CSS_SELECTOR, "a.block.truncate.font-500.mb-4.hover\\:c-red.pb-2"))
                        )
                        print("找到搜索结果链接，正在点击...")
                        # 滚动元素到视窗中间位置，避免被头部遮挡
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", result_link)
                        time.sleep(1)
                        # 尝试直接点击
                        try:
                            result_link.click()
                            print("已成功点击搜索结果链接")
                        except Exception as click_error:
                            # 如果直接点击失败，使用JavaScript点击
                            print("直接点击失败，使用JavaScript点击...")
                            self.driver.execute_script("arguments[0].click();", result_link)
                            print("已成功通过JavaScript点击搜索结果链接")
                        break
                    except Exception as e1:
                        print(f"方法1失败: {str(e1)}")
                        # 方法2: 使用JavaScript点击并确保元素在视图中
                        try:
                            result_link = self.driver.find_element(
                                By.CSS_SELECTOR, "a.block.truncate.font-500.mb-4.hover\\:c-red.pb-2")
                            print("使用JavaScript滚动并点击搜索结果链接...")
                            # 确保元素完全可见
                            self.driver.execute_script("""
                                arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'center'});
                                window.scrollBy(0, -100);  // 额外向上滚动一点，避免被头部遮挡
                            """, result_link)
                            time.sleep(1)
                            self.driver.execute_script("arguments[0].click();", result_link)
                            print("已成功通过JavaScript点击搜索结果链接")
                            break
                        except Exception as e2:
                            print(f"方法2失败: {str(e2)}")
                            # 方法3: 查找页面上任何包含基金代码的链接
                            try:
                                result_links = self.driver.find_elements(By.TAG_NAME, "a")
                                for link in result_links:
                                    if fund_code in link.get_attribute("href") or fund_code in link.text:
                                        print(f"找到包含基金代码的链接: {link.text}")
                                        # 确保元素可见并点击
                                        self.driver.execute_script("""
                                            arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'center'});
                                            window.scrollBy(0, -100);  // 额外向上滚动一点
                                        """, link)
                                        time.sleep(1)
                                        self.driver.execute_script("arguments[0].click();", link)
                                        print("已成功点击包含基金代码的链接")
                                        result_link = link
                                        break
                                if result_link:
                                    break
                            except Exception as e3:
                                print(f"方法3失败: {str(e3)}")
                    if attempt < max_attempts - 1:
                        print(f"第 {attempt + 1} 次尝试失败，等待后重试...")
                        time.sleep(2)
                except Exception as e:
                    print(f"尝试点击搜索结果时出现错误: {str(e)}")
                    if attempt < max_attempts - 1:
                        time.sleep(2)
            if not result_link:
                print("未能成功点击搜索结果链接，尝试直接访问基金页面...")
            # 等待新页面加载并切换到新页面
            time.sleep(5)  # 增加等待时间以应对页面加载较慢的情况
            # 获取所有窗口句柄
            all_windows = self.driver.window_handles
            current_window = self.driver.current_window_handle
            # 切换到新打开的窗口
            # for window in all_windows:
            #     if window != current_window:
            #         self.driver.switch_to.window(window)
            #         break
            self.driver.switch_to.window(all_windows[-1])
            # 等待页面加载完成，最多等待30秒
            try:
                WebDriverWait(self.driver, 30).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                print("页面加载完成")
            except Exception as e:
                print(f"页面加载超时，但将继续执行: {str(e)}")

            print(f"已切换到新页面，当前URL: {self.driver.current_url}")
            return True
        except Exception as e:
            print(f"搜索基金时出现错误: {str(e)}")
            return False

    def extract_data(self, fund_code, fund_data_file_path="fund_data.json"):
        """
        提取基金数据（回撤数据和区间收益数据）
        :param fund_code: 基金代码
        :param fund_data_file_path: 基金数据文件路径
        """
        try:
            time.sleep(2)
            # 打印当前页面URL
            print(f"当前页面URL: {self.driver.current_url}")
            # 获取动态回撤数据
            table = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                                                "div.el-table--fit.el-table--enable-row-hover.el-table--enable-row-transition.el-table.el-table--layout-fixed.mt-16.header-blue.is-scrolling-none"))
            )
            drawdown_data = parse_drawdown_data(driver=self.driver)
            print("提取的回撤数据:")
            for period, value in drawdown_data.items():
                print(f"  {period}: {value}")
            # 获取区间收益
            interval_return_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//aside[@id='nav-el-m2QCkZj']//div[contains(text(), '区间收益')]"))
            )
            # 点击标签
            interval_return_tab.click()
            print("已点击'区间收益'标签")
            # 等待页面内容更新
            time.sleep(2)
            print("页面内容更新完成")
            # 等待并点击"阶段收益"标签
            stage_return_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//div[contains(text(), '阶段收益') and @class='xp-nav-item xs-nav-block-item']"))
            )
            stage_return_tab.click()
            print("已点击'阶段收益'标签")
            # 等待页面内容更新
            time.sleep(2)
            print("阶段收益页面内容更新完成")
            # 提取"阶段收益"表格数据
            # 等待包含数据的外部容器加载完成
            outer_container = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "aside[data-v-246b1dcb]"))
            )
            # 在外部容器同级查找下一个aside容器
            inner_aside_container = outer_container.find_element(By.XPATH,
                                                                 "./following-sibling::aside")
            # 在内部aside容器中查找div.el-table--fit容器
            table_container = inner_aside_container.find_element(By.CSS_SELECTOR,
                                                                 "div.el-table--fit")
            # 在表格容器中查找表格主体
            table = table_container.find_element(By.CSS_SELECTOR,
                                                 "table.el-table__body")
            # 获取所有行
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            print(f"找到 {len(rows)} 行数据")
            data_list = []
            # 遍历每一行提取数据
            for i, row in enumerate(rows):
                # 获取所有单元格
                cells = row.find_elements(By.CSS_SELECTOR, "td")
                if len(cells) >= 5:  # 确保有足够的列
                    # 提取区间
                    interval = cells[0].find_element(By.CSS_SELECTOR, ".cell").text
                    # 提取基金收益
                    fund_return_element = cells[1].find_element(By.CSS_SELECTOR,
                                                                "div")
                    fund_return = fund_return_element.text
                    # 提取业绩比较基准
                    benchmark_element = cells[2].find_element(By.CSS_SELECTOR,
                                                              "div")
                    benchmark = benchmark_element.text
                    # 提取超额收益(几何)
                    excess_return_element = cells[3].find_element(By.CSS_SELECTOR,
                                                                  "div")
                    excess_return = excess_return_element.text
                    # 提取同类平均
                    average_element = cells[4].find_element(By.CSS_SELECTOR, "div")
                    average = average_element.text

                    # 构造数据字典
                    data_row = {
                        "区间": interval,
                        "基金收益": {
                            "百分比": fund_return,
                        },
                        "业绩比较基准": {
                            "百分比": benchmark,
                        },
                        "超额收益(几何)": {
                            "百分比": excess_return,
                        },
                        "同类平均": {
                            "百分比": average,
                        }
                    }
                    # 添加到列表
                    data_list.append(data_row)
                    # 打印提取的数据
                    print(f"第{i + 1}行数据:")
                    print(f"  区间: {interval}")
                    print(f"  基金收益: {fund_return}")
                    print(f"  业绩比较基准: {benchmark} ")
                    print(f"  超额收益(几何): {excess_return}")
                    print(f"  同类平均: {average} ")
                    print("-" * 50)
            # 将数据保存到JSON文件
            if data_list:
                # 创建一个包含所有基金数据的字典
                all_fund_data = {}
                # 如果已存在基金数据文件，则读取现有数据
                if os.path.exists(fund_data_file_path):
                    try:
                        with open(fund_data_file_path, 'r', encoding='utf-8') as f:
                            all_fund_data = json.load(f)
                    except Exception as e:
                        print(f"读取现有基金数据文件时出错: {str(e)}")
                        # 如果读取失败，使用空字典
                        all_fund_data = {}
                
                # 确保基金代码在数据结构中存在
                if fund_code not in all_fund_data:
                    all_fund_data[fund_code] = {}
                
                # 将回撤数据和区间收益数据整合到同一个数据结构中
                all_fund_data[fund_code]["区间收益"] = data_list
                all_fund_data[fund_code]["回撤数据"] = drawdown_data
                
                # 保存所有基金数据到指定文件中
                with open(fund_data_file_path, 'w', encoding='utf-8') as f:
                    json.dump(all_fund_data, f, ensure_ascii=False, indent=4)
                print(f"数据已保存到 {fund_data_file_path}")
            return True
        except Exception as e:
            print(f"提取数据时出现错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def close(self):
        """
        关闭浏览器
        """
        if self.driver:
            self.driver.quit()


def simuwang(code, fund_data_file_path="fund_data.json"):
    browser = SimuwangBrowser()
    try:
        # 打开网站
        if not browser.open_simuwang():
            return
        # 登录
        if not browser.login():
            return

        # 支持查询单只或多只基金
        fund_codes = []
        if isinstance(code, str):
            fund_codes = [code]
        elif isinstance(code, list):
            fund_codes = code
        else:
            print("无效的基金代码格式")
            return

        # 遍历查询每只基金
        for fund_code in fund_codes:
            print(f"\n开始查询基金: {fund_code}")
            # 搜索基金
            if not browser.search_fund(fund_code):
                print(f"查询基金 {fund_code} 失败")
                continue
            # 提取数据，并传递基金数据文件路径
            browser.extract_data(fund_code, fund_data_file_path)
            # 在查询下一只基金前等待一段时间
            if fund_code != fund_codes[-1]:
                print(f"等待5秒后查询下一只基金...")
                time.sleep(5)
    except Exception as e:
        print(f"程序执行过程中出现错误: {str(e)}")
    finally:
        # 关闭浏览器
        browser.close()


if __name__ == '__main__':
    # 支持单只基金查询
    # code = '320016'

    # 支持多只基金查询
    codes = ['320016', '015382', '013876']  # 示例基金代码列表
    simuwang(codes)