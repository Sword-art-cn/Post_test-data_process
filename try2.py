import requests
import hashlib
import hmac
import json
import urllib.parse
import time
from datetime import datetime, timezone


class BaiduBCEAuth:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key
        self.auth_version = "1"
        self.expiration_in_seconds = "1800"

    def get_timestamp(self):
        """获取当前时间戳，格式如：2023-08-01T06:12:00Z"""
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")

    def normalize(self, string, encoding_slash=True):
        """规范化字符串，用于签名"""
        if string is None:
            return ""

        # 先进行URL编码
        result = urllib.parse.quote(string, safe='')

        # 替换特定字符
        replacements = {
            '!': '%21',
            '\'': '%27',
            '(': '%28',
            ')': '%29',
            '*': '%2A'
        }
        for char, replacement in replacements.items():
            result = result.replace(char, replacement)

        # 处理斜杠
        if not encoding_slash:
            result = result.replace('%2F', '/')

        return result

    def generate_canonical_uri(self, url_path):
        """生成规范化URI"""
        if not url_path:
            return ""

        # 分割路径并规范化每个部分
        path_parts = [part for part in url_path.split('/') if part]
        normalized_parts = [self.normalize(part) for part in path_parts]
        return '/' + '/'.join(normalized_parts)

    def generate_canonical_query_string(self, query_params):
        """生成规范化查询字符串"""
        normalized_params = []

        for key, value in sorted(query_params.items()):
            if key.lower() == "authorization":
                continue
            normalized_key = self.normalize(key)
            normalized_value = self.normalize(value) if value is not None else ""
            normalized_params.append(f"{normalized_key}={normalized_value}")

        return '&'.join(normalized_params)

    def generate_canonical_headers(self, headers):
        """生成规范化头信息"""
        # 默认需要包含的头部
        default_headers = ["host", "content-length", "content-type", "content-md5"]

        # 添加x-bce-date头部
        if 'x-bce-date' not in headers:
            headers['x-bce-date'] = self.get_timestamp()

        # 收集所有需要签名的头部
        signed_headers = []
        for header in default_headers:
            if header in headers:
                signed_headers.append(header)

        # 添加所有x-bce-开头的头部
        for header in headers:
            if header.lower().startswith('x-bce-'):
                signed_headers.append(header.lower())

        # 确保host头部存在
        if 'host' not in signed_headers:
            signed_headers.append('host')

        # 生成规范化头部字符串
        canonical_headers = []
        signed_headers = sorted(list(set(signed_headers)))  # 去重并排序

        for header in signed_headers:
            value = headers.get(header, "").strip()
            if value:
                normalized_header = self.normalize(header.lower())
                normalized_value = self.normalize(value)
                canonical_headers.append(f"{normalized_header}:{normalized_value}")

        return '\n'.join(canonical_headers), ';'.join(sorted(signed_headers))

    def generate_signature(self, method, url, headers, body=""):
        """生成签名"""
        # 解析URL
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        query_params = dict(urllib.parse.parse_qsl(parsed_url.query))

        # 确保host头部存在
        if 'host' not in headers:
            headers['host'] = parsed_url.netloc

        # 生成各个部分
        timestamp = self.get_timestamp()
        canonical_uri = self.generate_canonical_uri(path)
        canonical_query_string = self.generate_canonical_query_string(query_params)
        canonical_headers, signed_headers = self.generate_canonical_headers(headers)

        # 生成签名密钥
        signing_key_str = f"bce-auth-v{self.auth_version}/{self.access_key}/{timestamp}/{self.expiration_in_seconds}"
        signing_key = hmac.new(
            self.secret_key.encode('utf-8'),
            signing_key_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        # 生成规范化请求
        canonical_request = f"{method.upper()}\n{canonical_uri}\n{canonical_query_string}\n{canonical_headers}"

        # 计算签名
        signature = hmac.new(
            signing_key.encode('utf-8'),
            canonical_request.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        # 生成最终Authorization头
        authorization = f"{signing_key_str}/{signed_headers}/{signature}"
        return authorization

# 解析API
def process_response_sum(response_text):
    result = {
        "interface_accessibility": 0,
        "volume": 0,
        "speedPoint": 0,
        "density": 0,
        "travelTime": 0,
        "delay": 0,
        "exist": 0,
        "fail": 0
    }

    try:
        data = json.loads(response_text)
        if isinstance(data, list) and len(data) > 0:
            result["interface_accessibility"] = 1
            print("成功获取json文件")
            first_item = data[0]

            # 检查是否存在 'stats' 字段，并且是列表
            stats = first_item.get("data", [{}])[0].get("stats", [])
            # if not file_urls:  # 如果 urls 是空字典 {}
            #     print("数据返回值为空")

            # 遍历 'stats' 列表，累加 'volume'
            for stat in stats:
                try:
                    result["volume"] += stat["volume"]
                    result["exist"] += 1
                except:
                    result["fail"] += 1
                    print("1")
                pass
                try:
                    result["speedPoint"] += stat["speedPoint"]
                    result["exist"] += 1
                except:
                    result["fail"] += 1
                    print("2")
                pass
                try:
                    result["density"] += stat["density"]
                    result["exist"] += 1
                except:
                    result["fail"] += 1
                    print("3")
                pass
                try:
                    result["travelTime"] += stat["travelTime"]
                    result["exist"] += 1
                except:
                    result["fail"] += 1
                    print("4")
                pass
                try:
                    result["delay"] += stat["delay"]
                    result["exist"] += 1
                    print("done")
                except:
                    result["fail"] += 1
                    print("5")

    except:
        print("fail")
        pass

    return result


# 使用示例
if __name__ == "__main__":
    # 配置您的密钥
    access_key = "62b3ba390b174fc79b59c076e2af4283"
    secret_key = "a1170cc9bfd64125b8c3d23fa5edb430"

    # 创建认证对象
    auth = BaiduBCEAuth(access_key, secret_key)

    # 请求参数
    url = "https://xsite.apigw.icvsc.net/v1/data/intersection/file"
    params = {
        "intersectionId": "757",
        "startTimestamp": "1746056057000",
        "endTimestamp": "1746056057001",
        "dataTypes": "TRAFFIC_FLOW_HISTORY "
    }
    headers = {
        "host": "data-tsdb-export.xcloud.baidu-int.com"
    }

    # 生成签名
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    authorization = auth.generate_signature("GET", full_url, headers)

    # 添加Authorization头
    headers["Authorization"] = authorization


    # # 发送请求
    # response = requests.get(url, params=params, headers=headers, verify=False)  # verify=False仅用于测试
    #
    # print("Status Code:", response.status_code)
    # print("Response:", response.text)


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
        print("第二次请求文件请求状态码:", file_response.status_code)

        # result = process_response_sum(file_response.text)
        # print(result)
        # print(result['exist'])
        # if result['exist'] == 0:
        #     print(f"返回值无有效数据")
        #
        # if not result['exist'] == 0:
        #     print("成功了")
        # else:
        #     print("完蛋了")



        # # 打印文件内容
        # print("文件内容:")
        # try:
        #     file_data = file_response.json()  # 如果是JSON格式
        #     print(json.dumps(file_data, indent=2, ensure_ascii=False))
        # except ValueError:
        #     print(file_response.text)  # 如果不是JSON格式



        # 将文件内容保存到本地
        output_file = "api_response1.json"
        with open(output_file, "w", encoding="utf-8") as f:
            try:
                json.dump(file_response.json(), f, indent=2, ensure_ascii=False)  # 如果是JSON
            except ValueError:
                f.write(file_response.text)  # 如果是纯文本

        print(f"数据已保存到: {output_file}")

