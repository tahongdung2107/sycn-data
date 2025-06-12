from api.nhanh.order import OrderService
from api.nhanh.bill import BillService
from api.nhanh.category import CategoryService
from api.nhanh.product import ProductService
import logging

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    try:
        # Khởi tạo các service
        order_service = OrderService()
        bill_service = BillService()
        category_service = CategoryService()
        product_service = ProductService()

        # Chạy đồng bộ categories
        logger.info("Bắt đầu đồng bộ categories...")
        product_service.run_demo()
        # category_service.run_demo()
        logger.info("Đồng bộ categories hoàn tất!")

        # Chạy đồng bộ orders và bills
        logger.info("Bắt đầu đồng bộ orders và bills...")
        # order_service.run_demo()
        # bill_service.run_demo()
        logger.info("Đồng bộ orders và bills hoàn tất!")

    except Exception as e:
        logger.error(f"Lỗi khi chạy đồng bộ: {str(e)}")
        raise

if __name__ == "__main__":
    main()

