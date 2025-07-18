import urllib.parse
import pandas as pd
import requests
import os
import time
from methods.baidu_auth import BaiduBCEAuth  # 认证类
from methods.response import process_response_sum  # 解析API

# 配置路径，全局接口
input_file_path = "intelli_intersection.csv"  # 输入文件路径
output_folder = "scan_results"  # 扫描结果文件夹
final_output_path = "road_detect_test_final.csv"  # 最终输出文件路径
max_retries = 1  # 最大重试次数
retry_delay = 1  # 重试延迟(秒)

# 创建结果文件夹
os.makedirs(output_folder, exist_ok=True)


# 初始化结果列
def initialize_columns(df, scan_info):
    """初始化结果列，添加扫描信息"""
    df["interface_accessibility"] = 0
    df["volume"] = 0
    df["speedPoint"] = 0
    df["density"] = 0
    df["travelTime"] = 0
    df["delay"] = 0
    df["exist"] = 0
    df["fail"] = 0
    df["start_time"] = scan_info["start_time"]
    df["end_time"] = scan_info["end_time"]
    df["scan_count"] = scan_info["scan_count"]
    return df


# 发送API请求
def send_request(intersection_id, auth, start_time, end_time):
    """发送API请求"""
    url = "https://xsite.apigw.icvsc.net/v1/data/intersection/file"
    params = {
        "intersectionId": str(intersection_id),
        "startTimestamp": start_time,  # 可根据需要调整
        "endTimestamp": end_time,  # 可根据需要调整
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
                print(f"路口 {intersection_id} 第 {attempt + 1} 次尝试失败: 第一次请求url返回值为空")

            # 下载每个数据文件
            for file_url in file_urls:
                print("下载数据文件:", file_url)

                # 第二次请求：直接获取文件内容（不需要签名，因为URL已包含签名）
                file_response = requests.get(file_url, verify=False)
                print("文件请求状态码:", file_response.status_code)

                if file_response.status_code == 200:
                    result = process_response_sum(file_response.text)
                    print("第二次请求处理结果:", result)
                    if result['exist'] == 0:
                        if attempt == max_retries - 1:
                            print_colored(
                                f"❌ 路口 {intersection_id} 全部尝试失败: 返回值无有效数据",
                                "1;31"  # 红色加粗
                            )
                            return result
                        else:
                            print_colored(
                                f"路口 {intersection_id} 第 {attempt + 1} 次尝试失败: 返回值无有效数据\n进行第{attempt + 2}次尝试...",
                                "1;35"  # 紫色加粗
                            )
                    if not result['exist'] == 0:
                        print_colored(
                            f"✅ 路口 {intersection_id} 返回数据保存成功\n",
                            "1;32"  # 绿色加粗
                        )
                        return result

        except Exception as e:
            print(f"路口 {intersection_id} 第 {attempt + 1} 次尝试失败: {str(e)}")

        if attempt < max_retries - 1:
            time.sleep(retry_delay)
        else:
            print(f"\n❌ 路口 {intersection_id} 所有尝试全部失败\n")

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


def one_timestamp_full_scan(start_time, end_time, scan_count):
    """执行单次扫描"""
    # 读取数据
    try:
        df = pd.read_csv(input_file_path, encoding='gbk')
    except Exception as e:
        print(f"无法读取输入文件: {str(e)}")
        return

    # 当前扫描结果保存目录
    scan_filename = f"scan_{scan_count}_{start_time}_{end_time}.csv"
    scan_filepath = os.path.join(output_folder, scan_filename)


    # 筛选activate=1的路口
    active_intersections = df[df["activate"] == 1].copy()

    if active_intersections.empty:
        print("没有找到activate=1的路口数据")
        return

    # 初始化结果列
    scan_info = {
        "start_time": start_time,
        "end_time": end_time,
        "scan_count": scan_count
    }
    active_intersections = initialize_columns(active_intersections, scan_info)

    # 创建认证对象
    auth = BaiduBCEAuth(
        access_key="62b3ba390b174fc79b59c076e2af4283",
        secret_key="a1170cc9bfd64125b8c3d23fa5edb430"
    )

    # 遍历每个路口并测试接口
    total = len(active_intersections)
    for i, (index, row) in enumerate(active_intersections.iterrows(), 1):
        intersection_id = row["intersection_id"]
        print_colored(
            f"\n正在处理路口 {intersection_id} ({i}/{total})...",
            "1;36"  # 青色加粗
        )

        # 发送请求并获取结果
        result = send_request(intersection_id, auth, start_time, end_time)

        # 更新结果
        for key, value in result.items():
            active_intersections.at[index, key] = value

        # 每处理5个路口保存一次进度
        if i % 5 == 0:
            active_intersections.to_csv(scan_filepath, index=False, encoding='gbk')
            print(f"已保存进度到 {scan_filepath}")

    # 保存最终结果
    try:
        active_intersections.to_csv(scan_filepath, index=False, encoding='gbk')
        print(f"所有路口处理完成，结果已保存到 {scan_filepath}")
    except Exception as e:
        print(f"保存结果文件失败: {str(e)}")


def merge_all_scans():
    """合并所有扫描结果"""
    all_dfs = []

    # 读取所有扫描结果文件
    for filename in os.listdir(output_folder):
        if filename.startswith("scan_") and filename.endswith(".csv"):
            filepath = os.path.join(output_folder, filename)
            try:
                df = pd.read_csv(filepath, encoding='gbk')
                all_dfs.append(df)
                print(f"已加载扫描结果: {filename}")
            except Exception as e:
                print(f"加载扫描结果 {filename} 失败: {str(e)}")

    if not all_dfs:
        print("没有找到任何扫描结果文件")
        return False

    # 合并所有数据
    merged_df = pd.concat(all_dfs, ignore_index=True)

    # 保存最终结果
    try:
        merged_df.to_csv(final_output_path, index=False, encoding='gbk')
        print(f"所有扫描结果已合并保存到 {final_output_path}")
        return True
    except Exception as e:
        print(f"保存最终结果失败: {str(e)}")
        return False
def print_colored(text, color_code):
    """打印彩色文本（支持换行）"""
    ansi_code = f"\033[{color_code}m"
    reset_code = "\033[0m"
    print(f"{ansi_code}{text}{reset_code}")


if __name__ == "__main__":
    # 时间参数配置
    begin_scan = 1752454800000  # 开始扫描时间帧（毫秒） 【1月1日9点:1735693200000】
    delay = 86400000  # 间隔一天
    end_scan = 1752627600001  # 结束扫描时间帧（毫秒）  【7月16日9点:1735693200000】
    scan_time = begin_scan  # 当前扫描时间帧
    full_scan_count = 0

    # 记录开始时间
    start_timer = time.time()

    # 执行多次扫描
    while scan_time <= end_scan:
        start_time = scan_time
        end_time = start_time + 1

        # 记录开始时间
        start_scan_timer = time.time()

        # 打印彩色扫描信息
        print_colored(
            f"当前扫描区间开始节点: {start_time}\n当前扫描区间结束节点: {end_time}",
            "1;32"  # 绿色加粗
        )

        # 执行单次扫描
        one_timestamp_full_scan(start_time, end_time, full_scan_count + 1)

        # 记录结束时间
        end_scan_timer = time.time()

        # 计算总耗时（秒）
        elapsed_scan_time = end_scan_timer - start_scan_timer
        full_scan_count += 1
        print_colored(
            f"\n当前扫描次数: {str(full_scan_count)}\n当前扫描耗时: {elapsed_scan_time:.4f}",
            "1;35"  # 紫色加粗
        )
        scan_time += delay

    # 记录结束时间
    end_timer = time.time()

    # 计算总耗时（秒）
    elapsed_time = end_timer - start_timer

    # 合并所有扫描结果
    print("\n开始合并所有扫描结果...")
    if merge_all_scans():
        print_colored(f"\n共完成扫描次数: {full_scan_count}\n共计耗时: {elapsed_time:.4f}", "1;32")  # 绿色加粗
    else:
        print_colored("\n扫描结果合并失败", "1;31")  # 红色加粗

