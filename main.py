from api.nhanh.order import OrderService
from api.nhanh.bill import BillService
from api.nhanh.category import CategoryService
from api.nhanh.product import ProductService
import logging
import schedule
import time
from datetime import datetime

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sync_orders_and_bills():
    """Job đồng bộ orders và bills mỗi 15 phút"""
    try:
        logger.info(f"Bắt đầu đồng bộ orders và bills lúc {datetime.now().strftime('%H:%M:%S')}...")
        order_service = OrderService()
        bill_service = BillService()
        order_service.run_demo()
        bill_service.run_demo()
        logger.info("Đồng bộ orders và bills hoàn tất!")
    except Exception as e:
        logger.error(f"Lỗi khi đồng bộ orders và bills: {str(e)}")

def sync_categories_and_products():
    """Job đồng bộ categories và products mỗi ngày lúc 01:00"""
    try:
        logger.info(f"Bắt đầu đồng bộ categories và products lúc {datetime.now().strftime('%H:%M:%S')}...")
        category_service = CategoryService()
        product_service = ProductService()
        product_service.run_demo()
        category_service.run_demo()
        logger.info("Đồng bộ categories và products hoàn tất!")
    except Exception as e:
        logger.error(f"Lỗi khi đồng bộ categories và products: {str(e)}")

def main():
    try:
        # Lên lịch chạy job đồng bộ orders và bills mỗi 15 phút
        schedule.every(15).minutes.do(sync_orders_and_bills)
        
        # Lên lịch chạy job đồng bộ categories và products mỗi ngày lúc 01:00
        schedule.every().day.at("23:00").do(sync_categories_and_products)

        logger.info("Đã lên lịch các job đồng bộ!")
        logger.info("- Orders và Bills: Chạy mỗi 15 phút")
        logger.info("- Categories và Products: Chạy mỗi ngày lúc 01:00")

        # Chạy vòng lặp vô hạn để thực thi các job đã lên lịch
        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as e:
        logger.error(f"Lỗi khi chạy đồng bộ: {str(e)}")
        raise

if __name__ == "__main__":
    main()

