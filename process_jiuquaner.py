import pandas as pd
import akshare as ak

def process_jiuquaner_with_fund_names(excel_path):
    """
    读取jiuquaner.xlsx文件，通过code列查找对应的基金简称，并将结果保存到新文件
    """
    # 读取jiuquaner.xlsx文件
    jiuquaner_df = pd.read_excel(excel_path)
    print(f"{excel_path} 中共有 {len(jiuquaner_df)} 条记录")

    # 获取基金列表数据
    fund_list = ak.fund_name_em()
    # 将基金代码格式化为6位数，不足的前面补0
    fund_list['基金代码'] = fund_list['基金代码'].apply(lambda x: str(x).zfill(6))
    # 将基金列表保存到CSV文件
    fund_list.to_excel('fund_list.xlsx', index=False)
    fund_list_df = pd.read_excel('fund_list.xlsx')
    print(f"fund_list.csv 中共有 {len(fund_list_df)} 条记录")
    
    # 创建一个字典用于快速查找基金简称
    # 注意基金代码在CSV中是数字格式，而在Excel中也是数字格式
    fund_code_to_name = {}
    for _, row in fund_list_df.iterrows():
        fund_code = int(row['基金代码'])  # 转换为整数格式
        fund_name = row['基金简称']
        fund_code_to_name[fund_code] = fund_name
    
    # 为jiuquaner数据添加基金简称列
    def get_fund_name(code):
        # 直接使用整数格式进行匹配
        return fund_code_to_name.get(int(code), "未找到")
    
    # 添加name列
    jiuquaner_df['name'] = jiuquaner_df['code'].apply(get_fund_name)
    
    # 将code列格式化为6位数，不足的前面补0
    jiuquaner_df['code'] = jiuquaner_df['code'].apply(lambda x: str(int(x)).zfill(6))
    
    # 保存到新文件
    output_filename = 'jiuquaner_with_names.xlsx'
    jiuquaner_df.to_excel(output_filename, index=False)
    print(f"已将匹配结果保存到 {output_filename} 文件中")
    
    # 显示匹配结果统计
    found_count = len(jiuquaner_df[jiuquaner_df['name'] != '未找到'])
    not_found_count = len(jiuquaner_df[jiuquaner_df['name'] == '未找到'])
    print(f"匹配成功: {found_count} 条记录")
    print(f"未找到: {not_found_count} 条记录")
    
    # 显示前几条匹配结果
    print("\n前10条匹配结果:")
    print(jiuquaner_df.head(10))
    
    # 显示匹配成功的记录
    if found_count > 0:
        print("\n匹配成功的记录:")
        matched = jiuquaner_df[jiuquaner_df['name'] != '未找到']
        print(matched)
    return output_filename

if __name__ == "__main__":
    process_jiuquaner_with_fund_names()