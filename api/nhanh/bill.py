from datetime import datetime
from service.fetchData import NhanhAPIClient
from service.createTable import create_table
from service.insertUpdateData import process_data

class BillService:
    def __init__(self):
        self.api_client = NhanhAPIClient()
        self.table_name = 'bills'  # Tên bảng mặc định

    def get_bills(self, path, start_date, end_date, params=None, step_days=None, items_per_page=100, 
                 date_from_field='fromDate', date_to_field='toDate', data_key='bills'):
        """
        Lấy hóa đơn từ API Nhanh với các tham số tùy chỉnh
        :param path: Đường dẫn API (ví dụ: /bill/search)
        :param start_date: Ngày bắt đầu (datetime object hoặc string format: YYYY-MM-DD)
        :param end_date: Ngày kết thúc (datetime object hoặc string format: YYYY-MM-DD)
        :param params: Dictionary chứa các tham số bổ sung
        :param step_days: Số ngày cho mỗi lần gọi API
        :param items_per_page: Số item trên mỗi trang
        :param date_from_field: Tên trường ngày bắt đầu
        :param date_to_field: Tên trường ngày kết thúc
        :param data_key: Tên key chứa dữ liệu trong response (mặc định là 'bills')
        :return: List các hóa đơn
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
        Chạy demo lấy hóa đơn và lưu vào database
        """
        # Lấy hóa đơn từ ngày 1-1-2025 đến hiện tại
        end_date = datetime.now()
        # start_date = datetime(2025, 1, 1)  # Ngày 1-1-2025
        start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        result = self.get_bills(
            path='/bill/search',
            start_date=start_date,
            end_date=end_date,
            params={},
            step_days=9,
            items_per_page=100,
            date_from_field='fromDate',
            date_to_field='toDate',
            data_key='bill'
        )
        
        if result and 'table' in result:
            # Tạo bảng nếu chưa tồn tại
            create_table(result['table'], self.table_name)
            # Thêm/cập nhật dữ liệu
            if 'data' in result and result['data']:
                process_data(result['data'], self.table_name)
            else:
                print("Không có dữ liệu để xử lý")
        else:
            print("Không tìm thấy cấu trúc bảng trong response")
