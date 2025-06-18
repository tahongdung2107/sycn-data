from datetime import datetime
from service.fetchData import NhanhAPIClient
from service.createTable import create_table
from service.insertUpdateData import process_data

class OrderService:
    def __init__(self):
        self.api_client = NhanhAPIClient()
        self.table_name = 'orders'  # Thêm tên bảng mặc định

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
        Chạy demo lấy đơn hàng và lưu vào database
        """
        # Lấy đơn hàng từ ngày 1-1-2025 đến hiện tại
        # end_date = datetime(2025, 3, 1)
        end_date = datetime.now()
        start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Ngày 1-1-2025
        # start_date = datetime(2024, 1, 1)
        
        result = self.get_orders(
            path='/order/index',
            start_date=start_date,
            end_date=end_date,
            params={},
            step_days=9,
            items_per_page=100,
            date_from_field='updatedDateTimeFrom',
            date_to_field='updatedDateTimeTo',
            data_key='orders'
        )
        
        if result and 'data' in result and result['data']:
            # In ra chi tiết về dữ liệu đơn hàng
            print("\nPhân tích dữ liệu đơn hàng:")
            total_orders = len(result['data'])
            orders_with_tags = sum(1 for order in result['data'] if order.get('tags'))
            orders_with_packed = sum(1 for order in result['data'] if order.get('packed') and order['packed'].get('id'))
            orders_with_facebook = sum(1 for order in result['data'] if order.get('facebook') and any(order['facebook'].values()))
            
            print(f"Tổng số đơn hàng: {total_orders}")
            print(f"Số đơn hàng có tags: {orders_with_tags}")
            print(f"Số đơn hàng có packed: {orders_with_packed}")
            print(f"Số đơn hàng có facebook: {orders_with_facebook}")
            
            # In ra mẫu dữ liệu của một số đơn hàng có dữ liệu
            print("\nMẫu dữ liệu đơn hàng có tags:")
            for order in result['data']:
                if order.get('tags'):
                    print(f"Order ID: {order.get('id')}")
                    print(f"Tags: {order['tags']}")
                    break
                    
            print("\nMẫu dữ liệu đơn hàng có packed:")
            for order in result['data']:
                if order.get('packed') and order['packed'].get('id'):
                    print(f"Order ID: {order.get('id')}")
                    print(f"Packed: {order['packed']}")
                    break
                    
            print("\nMẫu dữ liệu đơn hàng có facebook:")
            for order in result['data']:
                if order.get('facebook') and any(order['facebook'].values()):
                    print(f"Order ID: {order.get('id')}")
                    print(f"Facebook: {order['facebook']}")
                    break
            
            # Tạo bảng nếu chưa tồn tại
            if 'table' in result:
                create_table(result['table'], self.table_name)
                # Thêm/cập nhật dữ liệu
                process_data(result['data'], self.table_name)
            else:
                print("Không tìm thấy cấu trúc bảng trong response")
        else:
            print("Không có dữ liệu để xử lý")


    
