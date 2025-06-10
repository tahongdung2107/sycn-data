import requests
import json
from datetime import datetime, timedelta

class NhanhAPIClient:
    def __init__(self):
        self.base_url = "https://open.nhanh.vn/api"
        self.headers = {
            'Cookie': 'nvnKn0x6mr3=v2rrflko1bk2osg9jmvju1f92b; opaBm2zrqT=dtj6ihc39fjopbltp072v4nmbq'
        }
        self.app_id = "75541"
        self.access_token = "WMHCj89gaaCEHM1xjv7kcpPhsAhMJimNIvOOQPucPjb81ZC3IYM7GatYvXVVI2Q7hc8gvlTxJVAgkjsQO1u2BBGpVcWTwbMJUHGSGpq00GNKWp8DG9uqFIZphprqCeXEOWyeeIWTRPELhKPOiQ7EPEe2boMIIRDJMOH81kuVDI47zABD9sWOX9YisgqtxajjFa3Nv1flxtXUmrDk77Wum9RPyHVBXoNVLGwDAmsSzdEkXogirUoyXRY4gkQO"
        self.business_id = "28099"

    def _analyze_data_structure(self, data, path=None, level=0):
        """
        Phân tích cấu trúc dữ liệu và trả về thông tin về các trường
        :param data: Dữ liệu cần phân tích
        :param path: Đường dẫn API (để xử lý đặc biệt cho một số API)
        :param level: Cấp độ lồng nhau của dữ liệu
        :return: List các thông tin về trường
        """
        if not data or not isinstance(data, list) or len(data) == 0:
            return []
            
        table_info = []
        seen_fields = {}  # Dictionary để theo dõi các trường đã xuất hiện
        
        def analyze_value(value, field_name=None):
            if isinstance(value, (int, float)):
                return "number"
            elif isinstance(value, bool):
                return "boolean"
            elif isinstance(value, str):
                return "string"
            elif isinstance(value, list):
                if len(value) > 0:
                    # Phân tích phần tử đầu tiên của mảng
                    first_item = value[0]
                    if isinstance(first_item, dict):
                        return {
                            "type": "array",
                            "items": self._analyze_data_structure([first_item], path, level + 1)
                        }
                    else:
                        return {
                            "type": "array",
                            "items": analyze_value(first_item)
                        }
                return "array"
            elif isinstance(value, dict):
                return {
                    "type": "object",
                    "properties": self._analyze_data_structure([value], path, level + 1)
                }
            else:
                return "unknown"
        
        # Phân tích tất cả các phần tử trong data
        for item in data:
            if not isinstance(item, dict):
                continue
                
            for key, value in item.items():
                # Bỏ qua nếu trường đã xuất hiện trước đó
                if key in seen_fields:
                    continue
                    
                type_info = analyze_value(value, key)
                table_info.append({
                    "field": key,
                    "type": type_info
                })
                seen_fields[key] = True  # Đánh dấu trường đã xuất hiện
                
                # Xử lý đệ quy cho trường childs
                if key == 'childs' and isinstance(value, list) and len(value) > 0:
                    child_table_info = self._analyze_data_structure(value, path, level + 1)
                    if child_table_info:
                        # Thêm thông tin cấu trúc của childs vào table_info
                        table_info.append({
                            "field": f"childs_{level + 1}",
                            "type": {
                                "type": "array",
                                "items": child_table_info
                            }
                        })
                
        return table_info

    def analyze_response(self, response_data):
        """
        Phân tích dữ liệu response và trả về thông tin cấu trúc
        :param response_data: Dữ liệu response từ API
        :return: Dictionary chứa data và table
        """
        if not response_data or 'data' not in response_data:
            return {
                "data": [],
                "table": []
            }

        data = response_data['data']
        
        # Phân tích cấu trúc dữ liệu và tạo thông tin bảng
        table_info = self._analyze_data_structure(data)
            
        return {
            "data": data,
            "table": table_info
        }

    def fetch_data(self, path, params=None):
        """
        Gọi API Nhanh với các tham số tùy chỉnh
        :param path: Đường dẫn API (ví dụ: /category/index)
        :param params: Dictionary chứa các tham số bổ sung
        :return: Dictionary chứa data và table
        """
        try:
            url = f"{self.base_url}{path}"
            
            # Chuẩn bị data
            data = {}
            
            # Thêm các tham số bổ sung nếu có
            if params:
                data.update(params)
            
            # Chuẩn bị form data
            form_data = {
                'version': '2.0',
                'appId': self.app_id,
                'accessToken': self.access_token,
                'businessId': self.business_id,
                'data': json.dumps(data)
            }
            
            # Gọi API
            response = requests.post(url, headers=self.headers, data=form_data)
            
            # Kiểm tra response
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 1:  # API Nhanh trả về code 1 khi thành công
                    return self.analyze_response(result)
                else:
                    print(f"Lỗi API: {result.get('messages', 'Unknown error')}")
                    return {
                        "data": [],
                        "table": []
                    }
            else:
                print(f"Lỗi HTTP: {response.status_code}")
                return {
                    "data": [],
                    "table": []
                }
                
        except Exception as e:
            print(f"Lỗi khi gọi API: {str(e)}")
            return {
                "data": [],
                "table": []
            }
