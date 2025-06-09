from datetime import datetime, timedelta
from service.fetchData import NhanhAPIClient
from service.createTable import create_table
import json

class OrderService:
    def __init__(self):
        self.api_client = NhanhAPIClient()

    def get_orders(self, path, start_date, end_date, params=None, step_days=None, items_per_page=100, 
                  date_from_field='updatedDateTimeFrom', date_to_field='updatedDateTimeTo', data_key='orders'):
        """
        Lấy đơn hàng từ API Nhanh với các tham số tùy chỉnh
        :param path: Đường dẫn API (ví dụ: /order/index)
        :param start_date: Ngày bắt đầu (datetime object hoặc string format: YYYY-MM-DD)
        :param end_date: Ngày kết thúc (datetime object hoặc string format: YYYY-MM-DD)
        :param params: Dictionary chứa các tham số bổ sung
        :param step_days: Số ngày cho mỗi lần gọi API
        :param items_per_page: Số item trên mỗi trang
        :param date_from_field: Tên trường ngày bắt đầu
        :param date_to_field: Tên trường ngày kết thúc
        :param data_key: Tên key chứa dữ liệu trong response (mặc định là 'orders')
        :return: List các đơn hàng
        """
        # Chuyển đổi ngày tháng sang định dạng chuỗi nếu là datetime object
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')

        return self.api_client.get_data_by_date_range(
            path, 
            start_date, 
            end_date, 
            params, 
            step_days, 
            items_per_page,
            date_from_field,
            date_to_field,
            data_key
        )

    def run_demo(self):
        """
        Chạy demo lấy đơn hàng và lưu vào file
        """
        # Lấy đơn hàng từ ngày 1-1-2025 đến hiện tại
        end_date = datetime(2025, 1, 1)
        start_date = datetime(2025, 1, 1)  # Ngày 1-1-2025
        
        # Tham số bổ sung
        params = {
            'status': 'pending'  # Ví dụ: lấy đơn hàng có trạng thái pending
        }
        
        result = self.get_orders(
            path='/order/index',
            start_date=start_date,
            end_date=end_date,
            params=params,
            step_days=9,
            items_per_page=100,
            date_from_field='updatedDateTimeFrom',
            date_to_field='updatedDateTimeTo',
            data_key='orders'
        )
        
        if result and 'table' in result:
            # print(result['table'])
            create_table(result['table'], 'orders')
        else:
            print("No table structure found in the response")


    
