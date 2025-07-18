import pandas as pd


def split_intersection_names(input_file_path, output_file_path):
    # 读取输入表格文件
    df = pd.read_csv(input_file_path)

    # 初始化两个新列
    df['路段1'] = ''
    df['路段2'] = ''

    # 遍历每一行，拆分路口名
    for index, row in df.iterrows():
        intersection_name = row['路口名']
        if '-' in intersection_name:
            road1, road2 = intersection_name.split('-', 1)  # 只分割第一个'-'
            df.at[index, '路段1'] = road1.strip()
            df.at[index, '路段2'] = road2.strip()

    # 保存到输出文件
    df.to_csv(output_file_path, index=False, encoding='gbk')  # 如果是Excel文件，使用 df.to_excel(output_file_path, index=False)




# 使用示例
input_file_path = '临时.csv'  # 替换为你的输入文件路径
output_file_path = '路口名拆分.csv'  # 替换为你的输出文件路径
split_intersection_names(input_file_path, output_file_path)