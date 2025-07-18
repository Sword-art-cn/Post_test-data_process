from methods.baidu_auth import BaiduBCEAuth  # 假设您已将上面的认证类保存为baidu_auth.py
import requests
import json
import urllib.parse

def process_response(response_text):
    """
    处理API返回数据，提取所需信息
    """
    result = {
        "interface_accessibility": 0,  # 默认失败
        "volume": None,
        "speedPoint": None,
        "density": None,
        "travelTime": None,
        "delay": None
    }

    try:
        # 解析JSON数据
        data = json.loads(response_text)

        # 检查是否有数据
        if isinstance(data, list) and len(data) > 0:
            result["interface_accessibility"] = 1  # 成功

            # 获取第一条数据
            first_item = data[0]

            # 提取所需字段
            result["volume"] = first_item.get("volume")
            result["speedPoint"] = first_item.get("speedPoint")
            result["density"] = first_item.get("density")
            result["travelTime"] = first_item.get("travelTime")
            result["delay"] = first_item.get("delay")

    except json.JSONDecodeError:
        print("错误：响应不是有效的JSON格式")
    except Exception as e:
        print(f"处理响应时发生错误: {str(e)}")

    return result


if __name__ == "__main__":
    # 配置您的密钥
    access_key = "62b3ba390b174fc79b59c076e2af4283"
    secret_key = "a1170cc9bfd64125b8c3d23fa5edb430"

    # 创建认证对象
    auth = BaiduBCEAuth(access_key, secret_key)

    # 请求参数
    url = "https://xsite.apigw.icvsc.net/v1/data/intersection/flow/history"
    params = {
        "intersectionId": "728",
        "startTime": "1751241600000",
        "endTime": "1751242200000"
    }
    headers = {
        "host": "data-tsdb-export.xcloud.baidu-int.com"
    }

    try:
        # 生成签名
        full_url = f"{url}?{urllib.parse.urlencode(params)}"
        authorization = auth.generate_signature("GET", full_url, headers)

        # 添加Authorization头
        headers["Authorization"] = authorization

        # 发送请求
        response = requests.get(url, params=params, headers=headers, verify=False)  # verify=False仅用于测试

        # 检查响应状态
        if response.status_code == 200:
            # 处理响应数据
            processed_data = process_response(response.text)

            # 打印结果
            print("接口访问状态:", "成功" if processed_data["interface_accessibility"] else "失败")
            print("第一条数据的关键指标:")
            print(f"  - 流量(volume): {processed_data['volume']}")
            print(f"  - 速度(speedPoint): {processed_data['speedPoint']}")
            print(f"  - 密度(density): {processed_data['density']}")
            print(f"  - 行程时间(travelTime): {processed_data['travelTime']}")
            print(f"  - 延误(delay): {processed_data['delay']}")

            # 如果需要，可以将结果保存为JSON文件
            with open("traffic_data_result.json", "w") as f:
                json.dump(processed_data, f, indent=2)
        else:
            print(f"请求失败，状态码: {response.status_code}")
            print("响应内容:", response.text)

    except requests.exceptions.RequestException as e:
        print(f"请求发生错误: {str(e)}")