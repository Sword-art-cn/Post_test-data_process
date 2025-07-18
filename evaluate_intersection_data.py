import pandas as pd


def evaluate_intersection_data(input_path, output_path):
    """
    统计每个路口的两个指标：
    1. interface_accessibility==1的行数
    2. exist!=0的行数
    """
    # 读取数据
    try:
        df = pd.read_csv(input_path, encoding='gbk')
    except Exception as e:
        print(f"无法读取输入文件: {str(e)}")
        return

    # 确保路口ID列存在
    if '路口ID' not in df.columns and 'intersection_id' not in df.columns:
        print("错误：数据中找不到路口ID列（可能列名不是'路口ID'或'intersection_id'）")
        return

    # 统一列名处理
    id_column = '路口ID' if '路口ID' in df.columns else 'intersection_id'

    # 创建统计结果DataFrame
    results = []

    # 统计指定路口范围(727-776)
    for intersection_id in range(727, 777):
        # 筛选当前路口数据
        intersection_data = df[df[id_column] == intersection_id]

        if len(intersection_data) == 0:
            # 如果没有该路口数据，记录为0
            results.append({
                '路口ID': intersection_id,
                '有效访问次数': 0,
                '有效访问率(%)': 0.0,
                '存在数据次数': 0,
                '数据有效率(%)': 0.0
            })
            continue

        # 计算指标
        accessible_count = len(intersection_data[intersection_data['interface_accessibility'] == 1])
        exist_count = len(intersection_data[intersection_data['exist'] != 0])

        # 计算数据有效率（避免除以零）
        accessible_rate = (accessible_count / len(intersection_data) * 100) if len(intersection_data) > 0 else 0.0
        success_rate = (exist_count / accessible_count * 100) if accessible_count > 0 else 0.0

        results.append({
            '路口ID': intersection_id,
            '有效访问次数': accessible_count,
            '有效访问率(%)': accessible_rate,
            '存在数据次数': exist_count,
            '数据有效率(%)': round(success_rate, 2)
        })

    # 转换为DataFrame
    result_df = pd.DataFrame(results)

    # 保存结果
    try:
        result_df.to_csv(output_path, index=False, encoding='gbk')
        print(f"统计结果已保存到: {output_path}")
        print("\n统计结果预览:")
        print(result_df.head())
    except Exception as e:
        print(f"保存结果文件失败: {str(e)}")


if __name__ == "__main__":
    input_path = "road_detect_test_final.csv"  # 输入文件路径
    output_path = "test_evaluation.csv"  # 输出文件路径

    evaluate_intersection_data(input_path, output_path)