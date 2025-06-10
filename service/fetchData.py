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

    def fetch_data(self, path, params=None, page=1, items_per_page=100, data_key='orders'):
        """
        Gọi API Nhanh với các tham số tùy chỉnh
        :param path: Đường dẫn API (ví dụ: /order/index)
        :param params: Dictionary chứa các tham số bổ sung
        :param page: Số trang
        :param items_per_page: Số item trên mỗi trang
        :param data_key: Tên key chứa dữ liệu trong response (mặc định là 'orders')
        :return: Dữ liệu trả về dạng JSON
        """
        try:
            url = f"{self.base_url}{path}"
            
            # Chuẩn bị data
            data = {
                "page": page,
                "icpp": items_per_page
            }
            
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
                    data = result.get('data', {})
                    # Nếu data là dictionary chứa các đơn hàng, chuyển thành list
                    if isinstance(data, dict) and data_key in data:
                        orders_dict = data[data_key]
                        if isinstance(orders_dict, dict):
                            return list(orders_dict.values())
                    return data
                else:
                    print(f"Lỗi API: {result.get('messages', 'Unknown error')}")
                    return None
            else:
                print(f"Lỗi HTTP: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Lỗi khi gọi API: {str(e)}")
            return None

    def get_data_by_date_range(self, path, start_date, end_date, params=None, step_days=None, items_per_page=100, date_from_field='updatedDateTimeFrom', date_to_field='updatedDateTimeTo', data_key='orders'):
        """
        Lấy dữ liệu trong khoảng thời gian
        :param path: Đường dẫn API
        :param start_date: Ngày bắt đầu (format: YYYY-MM-DD)
        :param end_date: Ngày kết thúc (format: YYYY-MM-DD)
        :param params: Dictionary chứa các tham số bổ sung
        :param step_days: Số ngày cho mỗi lần gọi API
        :param items_per_page: Số item trên mỗi trang
        :param date_from_field: Tên trường ngày bắt đầu
        :param date_to_field: Tên trường ngày kết thúc
        :param data_key: Tên key chứa dữ liệu trong response (mặc định là 'orders')
        :return: List các dữ liệu
        """
        # Chuẩn bị tham số
        query_params = params.copy() if params else {}
        query_params.update({
            date_from_field: start_date,
            date_to_field: end_date
        })
        
        return self.get_all_data(path, query_params, step_days, items_per_page, date_from_field, date_to_field, data_key)

    def _analyze_data_structure(self, data):
        """
        Phân tích cấu trúc dữ liệu và trả về thông tin về các trường
        :param data: Dữ liệu cần phân tích
        :return: List các thông tin về trường
        """
        if not data or not isinstance(data, list) or len(data) == 0:
            return []
            
        # Lấy mẫu dữ liệu đầu tiên để phân tích
        sample = data[0]
        table_info = []
        seen_fields = {}  # Dictionary để theo dõi các trường đã xuất hiện
        
        def analyze_value(value):
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
                            "items": self._analyze_data_structure([first_item])
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
                    "properties": self._analyze_data_structure([value])
                }
            else:
                return "unknown"
        
        for key, value in sample.items():
            # Bỏ qua nếu trường đã xuất hiện trước đó
            if key in seen_fields:
                continue
                
            type_info = analyze_value(value)
            table_info.append({
                "field": key,
                "type": type_info
            })
            seen_fields[key] = True  # Đánh dấu trường đã xuất hiện
            
        return table_info

    def get_all_data(self, path, params=None, step_days=None, items_per_page=100, date_from_field='updatedDateTimeFrom', date_to_field='updatedDateTimeTo', data_key='orders'):
        """
        Lấy tất cả dữ liệu trong khoảng thời gian
        :param path: Đường dẫn API
        :param params: Dictionary chứa các tham số
        :param step_days: Số ngày cho mỗi lần gọi API (nếu có)
        :param items_per_page: Số item trên mỗi trang
        :param date_from_field: Tên trường ngày bắt đầu
        :param date_to_field: Tên trường ngày kết thúc
        :param data_key: Tên key chứa dữ liệu trong response (mặc định là 'orders')
        :return: Dictionary chứa data và table
        """
        all_data = []
        page = 1
        total_items = 0

        # Xử lý tham số thời gian
        if params and date_from_field in params and date_to_field in params:
            start_date = datetime.strptime(params[date_from_field], '%Y-%m-%d')
            end_date = datetime.strptime(params[date_to_field], '%Y-%m-%d')
            
            if step_days:
                current_start = start_date
                while current_start <= end_date:
                    # Tính ngày kết thúc cho khoảng thời gian hiện tại
                    current_end = min(current_start + timedelta(days=step_days), end_date)
                    
                    print(f"\nĐang xử lý khoảng thời gian: {current_start.strftime('%Y-%m-%d')} đến {current_end.strftime('%Y-%m-%d')}")
                    
                    # Cập nhật params với khoảng thời gian mới
                    current_params = params.copy()
                    current_params[date_from_field] = current_start.strftime('%Y-%m-%d')
                    current_params[date_to_field] = current_end.strftime('%Y-%m-%d')
                    
                    # Lấy dữ liệu cho khoảng thời gian này
                    while True:
                        print(f"Đang lấy dữ liệu trang {page}...")
                        result = self.fetch_data(path, current_params, page, items_per_page, data_key)
                        if not result:
                            print("Không có dữ liệu trả về")
                            break
                            
                        # Xử lý dữ liệu trả về
                        if isinstance(result, list):
                            data = result
                        elif isinstance(result, dict):
                            data = result.get(data_key, [])
                        else:
                            data = []
                            
                        if not data:
                            print("Không có dữ liệu trong kết quả")
                            break
                            
                        print(f"Đã nhận được {len(data)} items")
                        all_data.extend(data)
                        total_items += len(data)
                        print(f"Tổng số items đã thêm: {total_items}")
                        
                        # Kiểm tra nếu số lượng items nhận được ít hơn items_per_page
                        if len(data) < items_per_page:
                            print("Đã đến trang cuối của khoảng thời gian này")
                            break
                            
                        page += 1
                    
                    # Cập nhật ngày bắt đầu cho lần lặp tiếp theo
                    current_start = current_end + timedelta(days=1)
                    page = 1  # Reset page về 1 cho khoảng thời gian mới
            else:
                # Không có step_days, lấy toàn bộ dữ liệu trong một lần
                print(f"\nĐang lấy dữ liệu cho toàn bộ khoảng thời gian: {start_date.strftime('%Y-%m-%d')} đến {end_date.strftime('%Y-%m-%d')}")
                while True:
                    print(f"Đang lấy dữ liệu trang {page}...")
                    result = self.fetch_data(path, params, page, items_per_page, data_key)
                    if not result:
                        print("Không có dữ liệu trả về")
                        break
                        
                    # Xử lý dữ liệu trả về
                    if isinstance(result, list):
                        data = result
                    elif isinstance(result, dict):
                        data = result.get(data_key, [])
                    else:
                        data = []
                        
                    if not data:
                        print("Không có dữ liệu trong kết quả")
                        break
                        
                    print(f"Đã nhận được {len(data)} items")
                    all_data.extend(data)
                    total_items += len(data)
                    print(f"Tổng số items đã thêm: {total_items}")
                    
                    # Kiểm tra nếu số lượng items nhận được ít hơn items_per_page
                    if len(data) < items_per_page:
                        print("Đã đến trang cuối")
                        break
                        
                    page += 1
        else:
            # Không có tham số thời gian, lấy toàn bộ dữ liệu
            print("\nĐang lấy toàn bộ dữ liệu không có điều kiện thời gian")
            while True:
                print(f"Đang lấy dữ liệu trang {page}...")
                result = self.fetch_data(path, params, page, items_per_page, data_key)
                if not result:
                    print("Không có dữ liệu trả về")
                    break
                    
                # Xử lý dữ liệu trả về
                if isinstance(result, list):
                    data = result
                elif isinstance(result, dict):
                    data = result.get(data_key, [])
                else:
                    data = []
                    
                if not data:
                    print("Không có dữ liệu trong kết quả")
                    break
                    
                print(f"Đã nhận được {len(data)} items")
                all_data.extend(data)
                total_items += len(data)
                print(f"Tổng số items đã thêm: {total_items}")
                
                # Kiểm tra nếu số lượng items nhận được ít hơn items_per_page
                if len(data) < items_per_page:
                    print("Đã đến trang cuối")
                    break
                    
                page += 1
            
        print(f"\nHoàn thành! Tổng số items đã lấy được: {total_items}")
        # Phân tích cấu trúc dữ liệu và tạo thông tin bảng
        table_info = self._analyze_data_structure(all_data)
            
        return {
            "data": all_data,
            "table": table_info
        }
