from api.nhanh.order import OrderService
from api.nhanh.bill import BillService

def main():
    # Khởi tạo OrderService và chạy demo
    # order_service = OrderService()
    # order_service.run_demo()
    bill_service = BillService()
    bill_service.run_demo()

if __name__ == "__main__":
    main()

