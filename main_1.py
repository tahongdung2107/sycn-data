from api.nhanh.order import OrderService
from api.nhanh.bill import BillService
from api.nhanh.category import CategoryService
from api.nhanh.product import ProductService
from api.nhanh.customer import CustomerService
from api.crm.api.customer import fetch_customer_data  # Thêm dòng này
from api.crm.api.pre_order import fetch_pre_order_data
from api.crm.api.pre_order_dr import fetch_pre_order_dr_data
from api.crm.api.sales import fetch_sales_data
import logging
from datetime import datetime
import pandas as pd
from service.insertUpdateData import delete_records_by_date

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sync_delete_and_reload_orders_bills():
    """Job xóa và reload orders, bills cho 2 tháng gần nhất lúc 00:00"""
    global is_critical_job_running
    is_critical_job_running = True
    try:
        logger.info(f"Bắt đầu xóa và reload orders, bills lúc {datetime.now().strftime('%H:%M:%S')}...")
        order_service = OrderService()
        bill_service = BillService()
        
        # Tính ngày bắt đầu và kết thúc
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = (end_date.replace(day=1) - pd.DateOffset(months=1)).replace(day=1)  # Lùi về đầu tháng trước 2 tháng
        start_date = start_date.to_pydatetime()
        
        # Chuyển ngày sang timestamp (milliseconds)
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        delete_records_by_date('orders', 'updatedAt', start_ts, end_ts)
        delete_records_by_date('bills', 'createdDateTime', start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        # Đồng bộ lại dữ liệu
        order_service.run_demo(start_date, end_date)
        bill_service.run_demo(start_date, end_date)
        # fetch_customer_data()
        logger.info("Đã xóa và reload orders, bills cho 2 tháng gần nhất!")
    except Exception as e:
        logger.error(f"Lỗi khi xóa và reload orders, bills: {str(e)}")
    finally:
        is_critical_job_running = False


def main():
    try:
        # Khởi tạo các service
        order_service = OrderService()
        bill_service = BillService()
        category_service = CategoryService()
        product_service = ProductService()
        customer_service = CustomerService()
        # sync_delete_and_reload_orders_bills()
        # Chạy đồng bộ categories
        logger.info("Bắt đầu đồng bộ categories...")
        # product_service.run_demo()
        # category_service.run_demo()
        logger.info("Đồng bộ categories hoàn tất!")

        # Chạy đồng bộ orders và bills
        logger.info("Bắt đầu đồng bộ orders và bills...")
        # order_service.run_demo()
        # bill_service.run_demo()
        logger.info("Đồng bộ orders và bills hoàn tất!")

        # Chạy đồng bộ customers
        logger.info("Bắt đầu đồng bộ customers...")
        # customer_service.run_demo()
        logger.info("Đồng bộ customers hoàn tất!")

        # --- Sử dụng hàm fetch_customer_data từ CRM ---
        # logger.info("Lấy dữ liệu customer từ CRM...")
        crm_customer = fetch_customer_data()
        # crm_pre_order = fetch_pre_order_data()
        # crm_pre_order_dr = fetch_pre_order_dr_data()
        # crm_sales = fetch_sales_data()
        # print("Kết quả từ CRM:")

    except Exception as e:
        logger.error(f"Lỗi khi chạy đồng bộ: {str(e)}")
        raise

if __name__ == "__main__":
    main()

