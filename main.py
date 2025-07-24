from api.nhanh.order import OrderService
from api.nhanh.bill import BillService
from api.nhanh.category import CategoryService
from api.nhanh.product import ProductService
from api.nhanh.customer import CustomerService
## CRM
from api.crm.api.customer import fetch_customer_data
from api.crm.api.pre_order import fetch_pre_order_data
from api.crm.api.pre_order_dr import fetch_pre_order_dr_data
from api.crm.api.sales import fetch_sales_data
import logging
import schedule
import time
from datetime import datetime
from service.insertUpdateData import delete_records_by_date
import pandas as pd

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Biến trạng thái kiểm soát job quan trọng
is_critical_job_running = False

def sync_orders_and_bills():
    """Job đồng bộ orders và bills mỗi 15 phút từ 05:00 đến 23:00"""
    global is_critical_job_running
    if is_critical_job_running:
        logger.info("Bỏ qua job đồng bộ orders và bills vì đang có job quan trọng chạy!")
        return
    
    # Kiểm tra thời gian hiện tại có trong khoảng 05:00-23:00 không
    current_hour = datetime.now().hour
    if current_hour < 5 or current_hour >= 23:
        logger.info(f"Bỏ qua job đồng bộ orders và bills vì ngoài giờ hoạt động (hiện tại: {current_hour}:00)")
        return
        
    try:
        logger.info(f"Bắt đầu đồng bộ orders và bills lúc {datetime.now().strftime('%H:%M:%S')}...")
        order_service = OrderService()
        bill_service = BillService()
        customer_service = CustomerService()
        order_service.run_demo()
        bill_service.run_demo()
        customer_service.run_demo()
        fetch_pre_order_data()
        fetch_pre_order_dr_data()
        logger.info("Đồng bộ orders và bills hoàn tất!")
    except Exception as e:
        logger.error(f"Lỗi khi đồng bộ orders và bills: {str(e)}")

def sync_categories_and_products():
    """Job đồng bộ categories và products mỗi ngày lúc 01:00"""
    global is_critical_job_running
    is_critical_job_running = True
    try:
        logger.info(f"Bắt đầu đồng bộ categories và products lúc {datetime.now().strftime('%H:%M:%S')}...")
        category_service = CategoryService()
        product_service = ProductService()
        product_service.run_demo()
        category_service.run_demo()
        fetch_sales_data()
        logger.info("Đồng bộ categories và products hoàn tất!")
    except Exception as e:
        logger.error(f"Lỗi khi đồng bộ categories và products: {str(e)}")
    finally:
        is_critical_job_running = False

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
        start_date = (end_date.replace(day=1) - pd.DateOffset(months=2)).replace(day=1)  # Lùi về đầu tháng trước 2 tháng
        start_date = start_date.to_pydatetime()
        
        # Chuyển ngày sang timestamp (milliseconds)
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        delete_records_by_date('orders', 'updatedAt', start_ts, end_ts)
        delete_records_by_date('bills', 'createdDateTime', start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        
        # Đồng bộ lại dữ liệu
        order_service.run_demo(start_date, end_date)
        bill_service.run_demo(start_date, end_date)
        fetch_customer_data()
        logger.info("Đã xóa và reload orders, bills cho 2 tháng gần nhất!")
    except Exception as e:
        logger.error(f"Lỗi khi xóa và reload orders, bills: {str(e)}")
    finally:
        is_critical_job_running = False

def main():
    try:
        # Lên lịch chạy job đồng bộ orders và bills mỗi 15 phút
        schedule.every(15).minutes.do(sync_orders_and_bills)
        
        # Lên lịch chạy job đồng bộ categories và products mỗi ngày lúc 01:00
        schedule.every().day.at("23:30").do(sync_categories_and_products)

        # Lên lịch chạy job xóa và reload orders, bills lúc 00:00 mỗi ngày
        schedule.every().day.at("00:10").do(sync_delete_and_reload_orders_bills)

        # Lên lịch chạy job đồng bộ customer CRM mỗi ngày lúc 00:00

        logger.info("Đã lên lịch các job đồng bộ!")
        logger.info("- Orders và Bills: Chạy mỗi 15 phút từ 05:00 đến 23:00")
        logger.info("- Categories và Products: Chạy mỗi ngày lúc 20:00")
        logger.info("- Xóa và Reload Orders và Bills: Chạy mỗi ngày lúc 00:00")

        # Chạy vòng lặp vô hạn để thực thi các job đã lên lịch
        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as e:
        logger.error(f"Lỗi khi chạy đồng bộ: {str(e)}")
        raise

if __name__ == "__main__":
    main()

 