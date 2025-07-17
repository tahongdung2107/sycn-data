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
        customer_service = CustomerService()

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
        logger.info("Lấy dữ liệu customer từ CRM...")
        # crm_customer = fetch_customer_data()
        # crm_pre_order = fetch_pre_order_data()
        # crm_pre_order_dr = fetch_pre_order_dr_data()
        crm_sales = fetch_sales_data()
        print("Kết quả từ CRM:")

    except Exception as e:
        logger.error(f"Lỗi khi chạy đồng bộ: {str(e)}")
        raise

if __name__ == "__main__":
    main()

