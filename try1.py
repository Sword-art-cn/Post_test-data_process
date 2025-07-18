import urllib.parse
import pandas as pd
import requests
import json
import time
from methods.baidu_auth import BaiduBCEAuth  # 认证类
from methods.response import process_response_sum  # 解析API

# 配置
input_file_path = "road_name_divide.csv"  # 输入文件路径
output_file_path = "road_detect_test.csv"  # 输出文件路径
max_retries = 5  # 最大重试次数
retry_delay = 1  # 重试延迟(秒)


# 初始化结果列
def initialize_columns(df):
    df["interface_accessibility"] = 0
    df["volume"] = 0
    df["speedPoint"] = 0
    df["density"] = 0
    df["travelTime"] = 0
    df["delay"] = 0
    df["exist"] = 0
    df["fail"] = 0
    return df

# 发送API请求
def send_request(intersection_id, auth):
    url = "https://xsite.apigw.icvsc.net/v1/data/intersection/file"
    params = {
        "intersectionId": str(intersection_id),
        "startTimestamp": "1746056057000",  # 可根据需要调整
        "endTimestamp": "1746056357000",  # 可根据需要调整
        "dataTypes": "TRAFFIC_FLOW_HISTORY "
    }
    headers = {
        "host": "data-tsdb-export.xcloud.baidu-int.com"
    }

    for attempt in range(max_retries):
        try:
            # 生成签名
            full_url = f"{url}?{urllib.parse.urlencode(params)}"
            authorization = auth.generate_signature("GET", full_url, headers)
            headers["Authorization"] = authorization

            # 发送第一次请求
            response = requests.get(url, params=params, headers=headers, verify=False)
            print("第一次请求状态码:", response.status_code)
            print("第一次请求内容:", response.text)

            # 解析响应获取文件URL
            data = response.json()
            file_urls = data.get("results", {}).get("TRAFFIC_FLOW", {}).get("urls", {}).values()
            if not file_urls:  # 如果 urls 是空字典 {}
                print("第一次请求url返回值为空")

            # 下载每个数据文件
            for file_url in file_urls:
                print("\n下载数据文件:", file_url)

                # 第二次请求：直接获取文件内容（不需要签名，因为URL已包含签名）
                file_response = requests.get(file_url, verify=False)
                print("文件请求状态码:", file_response.status_code)

                if file_response.status_code == 200:
                    result = process_response_sum(file_response.text)
                    print(result)
                    return result

        except Exception as e:
            print(f"路口 {intersection_id} 第 {attempt + 1} 次尝试失败: {str(e)}")

        if attempt < max_retries - 1:
            time.sleep(retry_delay)

    return {
        "interface_accessibility": 0,
        "volume": 0,
        "speedPoint": 0,
        "density": 0,
        "travelTime": 0,
        "delay": 0,
        "exist": 0,
        "fail": 0
    }


def main():
    # 读取数据
    try:
        df = pd.read_csv(input_file_path)
    except Exception as e:
        print(f"无法读取输入文件: {str(e)}")
        return

    # 初始化结果列
    df = initialize_columns(df)

    # 筛选activate=1的路口
    active_intersections = df[df["activate"] == 1].copy()

    if active_intersections.empty:
        print("没有找到activate=1的路口数据")
        return

    # 创建认证对象
    auth = BaiduBCEAuth(
        access_key="62b3ba390b174fc79b59c076e2af4283",
        secret_key="a1170cc9bfd64125b8c3d23fa5edb430"
    )

    # 遍历每个路口并测试接口
    total = len(active_intersections)
    for i, (index, row) in enumerate(active_intersections.iterrows(), 1):
        intersection_id = row["路口ID"]
        print(f"正在处理路口 {intersection_id} ({i}/{total})...")

        # 发送请求并获取结果
        result = send_request(intersection_id, auth)


        # 更新结果
        for key, value in result.items():
            df.at[index, key] = value

        # 每处理5个路口保存一次进度
        if i % 5 == 0:
            df.to_csv(output_file_path, index=False, encoding='gbk')
            print(f"已保存进度到 {output_file_path}")

    # 保存最终结果
    try:
        df.to_csv(output_file_path, index=False, encoding='gbk')
        print(f"所有路口处理完成，结果已保存到 {output_file_path}")
    except Exception as e:
        print(f"保存结果文件失败: {str(e)}")


if __name__ == "__main__":
    main()