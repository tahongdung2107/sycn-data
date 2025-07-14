import requests
from typing import List, Dict, Any, Tuple
import logging
from datetime import datetime
import sys
import os
import json
import uuid
import traceback
# Thêm đường dẫn thư mục gốc vào PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from service.fetchData import NhanhAPIClient
from service.createTable import create_table

from database.manager import DatabaseManager

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerService:
    def __init__(self):
        """Khởi tạo class với DatabaseManager và NhanhAPIClient"""
        self.db = DatabaseManager()
        self.api_client = NhanhAPIClient()
        self.table_name = "customers"

    def create_table(self):
        """Tạo bảng customers với cấu trúc phù hợp với dữ liệu từ API"""
        # Cấu trúc bảng customers
        customer_columns = {
            "id": "NVARCHAR(50) PRIMARY KEY",
            "type": "NVARCHAR(10)",
            "name": "NVARCHAR(255)",
            "mobile": "NVARCHAR(20)",
            "gender": "NVARCHAR(10)",
            "email": "NVARCHAR(255)",
            "address": "NVARCHAR(MAX)",
            "birthday": "DATE",
            "code": "NVARCHAR(50)",
            "level": "NVARCHAR(50)",
            "customer_group": "NVARCHAR(255)",
            "levelId": "NVARCHAR(50)",
            "groupId": "NVARCHAR(50)",
            "cityLocationId": "NVARCHAR(50)",
            "districtLocationId": "NVARCHAR(50)",
            "wardLocationId": "NVARCHAR(50)",
            "totalMoney": "DECIMAL(18,2)",
            "startedDate": "DATE",
            "startedDepotId": "NVARCHAR(50)",
            "points": "INT",
            "totalBills": "INT",
            "lastBoughtDate": "DATE",
            "taxCode": "NVARCHAR(50)",
            "businessName": "NVARCHAR(255)",
            "businessAddress": "NVARCHAR(MAX)",
            "description": "NVARCHAR(MAX)",
            "created_at": "DATETIME DEFAULT GETDATE()",
            "updated_at": "DATETIME DEFAULT GETDATE()"
        }
        
        try:
            if not self.db.connect():
                raise Exception("Không thể kết nối đến database")
                
            if not self.db.create_table(self.table_name, customer_columns):
                raise Exception(f"Không thể tạo bảng {self.table_name}")
            
            logger.info("Tạo bảng customers thành công")
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo bảng: {str(e)}")
            raise
        finally:
            self.db.close()

    def get_customers(self, start_date: datetime = None, end_date: datetime = None, 
                     params: Dict = None, step_days: int = None, items_per_page: int = 100,
                     date_from_field: str = 'updatedDateTimeFrom', 
                     date_to_field: str = 'updatedDateTimeTo',
                     data_key: str = 'customers') -> Dict[str, Any]:
        """
        Lấy dữ liệu khách hàng từ API Nhanh.vn theo khoảng thời gian
        """
        # Nếu không có ngày bắt đầu và kết thúc, lấy tất cả khách hàng
        if not start_date:
            start_date = datetime(2025, 1, 1)  # Ngày mặc định
        if not end_date:
            end_date = datetime.now()
            
        # Chuyển đổi ngày tháng sang định dạng chuỗi nếu là datetime object
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')
        
        try:
            result = self.api_client.get_data_by_date_range(
                path='/customer/search',
                start_date=start_date,
                end_date=end_date,
                params=params or {},
                step_days=step_days,
                items_per_page=items_per_page,
                date_from_field=date_from_field,
                date_to_field=date_to_field,
                data_key=data_key
            )
            
            if not result:
                logger.error("API không trả về dữ liệu")
                return {}
                
            # Kiểm tra kiểu dữ liệu trả về
            if isinstance(result, list):
                # Chuyển đổi list thành dictionary với key là id
                customers_dict = {}
                for customer in result:
                    if isinstance(customer, dict) and 'id' in customer:
                        customers_dict[str(customer['id'])] = customer
                return {'data': {'customers': customers_dict}}
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi gọi API: {str(e)}")
            raise

    def sync_customers(self, start_date: datetime = None, end_date: datetime = None,
                      step_days: int = None, items_per_page: int = 100):
        """
        Đồng bộ khách hàng từ Nhanh.vn vào database theo khoảng thời gian
        """
        try:
            # Tạo bảng nếu chưa tồn tại
            self.create_table()
            
            # Lấy dữ liệu từ API
            result = self.get_customers(
                start_date=start_date,
                end_date=end_date,
                step_days=step_days,
                items_per_page=items_per_page
            )
            if not isinstance(result, dict):
                logger.warning("Dữ liệu trả về không phải dictionary")
                return
                
            data = result.get('data', [])
            if not isinstance(data, list):
                logger.warning("Dữ liệu data không phải list")
                return
                
            customers = data
            if customers:                
                # Debug: In ra mẫu dữ liệu đầu tiên
                if customers:
                    first_customer = customers[0]
                
                # Xử lý dữ liệu trước khi lưu vào database
                processed_customers = self.process_customer_data(customers)
                
                
                if processed_customers:
                    try:
                        # Kiểm tra số lượng khách hàng trùng lặp
                        customer_ids = set()
                        duplicate_ids = set()
                        for customer in processed_customers:
                            customer_id = customer.get('id')
                            if customer_id in customer_ids:
                                duplicate_ids.add(customer_id)
                            customer_ids.add(customer_id)
                        
                        if duplicate_ids:
                            logger.warning(f"Phát hiện {len(duplicate_ids)} ID khách hàng trùng lặp")
                            logger.warning(f"Danh sách ID trùng: {list(duplicate_ids)[:10]}...")
                        
                        # Lưu dữ liệu vào database
                        if not self.db.connect():
                            raise Exception("Không thể kết nối đến database")
                        try:
                            for customer in processed_customers:
                                customer_id = customer.get('id')
                                
                                # Kiểm tra xem khách hàng đã tồn tại chưa
                                check_query = f"SELECT COUNT(*) FROM {self.table_name} WHERE id = ?"
                                self.db.cursor.execute(check_query, [customer_id])
                                exists = self.db.cursor.fetchone()[0] > 0
                                
                                if exists:
                                    # UPDATE nếu đã tồn tại
                                    columns = list(customer.keys())
                                    update_clause = ', '.join([f"{col} = ?" for col in columns if col != 'id'])
                                    update_values = [customer[col] for col in columns if col != 'id'] + [customer_id]
                                    
                                    update_query = f"UPDATE {self.table_name} SET {update_clause} WHERE id = ?"
                                    self.db.cursor.execute(update_query, update_values)
                                    logger.info(f"Đã UPDATE khách hàng ID: {customer_id}")
                                else:
                                    # INSERT nếu chưa tồn tại
                                    columns = list(customer.keys())
                                    placeholders = ', '.join(['?' for _ in columns])
                                    column_names = ', '.join(columns)
                                    insert_values = [customer[col] for col in columns]
                                    
                                    insert_query = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})"
                                    self.db.cursor.execute(insert_query, insert_values)
                                    logger.info(f"Đã INSERT khách hàng mới ID: {customer_id}")
                                    
                            self.db.conn.commit()
                            logger.info(f"Hoàn thành! Tổng số khách hàng đã xử lý: {len(processed_customers)}")
                        except Exception as e:
                            logger.error(f"Lỗi khi insert/update customer {customer.get('id')}: {e}, data={customer}")
                            raise
                        finally:
                            self.db.close()
                        
                    except Exception as e:
                        logger.error(f"Lỗi khi lưu dữ liệu: {str(e)}")
                        raise
                else:
                    logger.warning("Không có khách hàng hợp lệ để xử lý")
            else:
                logger.warning("Không có dữ liệu khách hàng để xử lý")
                
        except Exception as e:
            logger.error(f"Lỗi khi đồng bộ khách hàng: {str(e)}")
            raise

    def process_customer_data(self, customers_data: List[Dict]) -> List[Dict[str, Any]]:
        """
        Xử lý dữ liệu khách hàng trước khi lưu vào database
        
        Args:
            customers_data: List chứa dữ liệu khách hàng
        Returns:
            List chứa dữ liệu khách hàng đã xử lý
        """
        processed_customers = []
        
        for customer in customers_data:
            try:
                # Đảm bảo customer là dictionary
                if isinstance(customer, str):
                    customer = json.loads(customer)
                elif not isinstance(customer, dict):
                    logger.warning(f"Bỏ qua khách hàng không phải dictionary: {customer}")
                    continue

                # Xử lý dữ liệu khách hàng
                processed_customer = {
                    "id": str(customer.get("id", "")),
                    "type": str(customer.get("type", "")),
                    "name": str(customer.get("name", "")),
                    "mobile": str(customer.get("mobile", "")),
                    "gender": str(customer.get("gender", "")) if customer.get("gender") else None,
                    "email": str(customer.get("email", "")) if customer.get("email") else None,
                    "address": str(customer.get("address", "")) if customer.get("address") else None,
                    "birthday": customer.get("birthday") if customer.get("birthday") else None,
                    "code": str(customer.get("code", "")) if customer.get("code") else None,
                    "level": str(customer.get("level", "")),
                    "customer_group": str(customer.get("group", "")),
                    "levelId": str(customer.get("levelId", "")),
                    "groupId": str(customer.get("groupId", "")),
                    "cityLocationId": str(customer.get("cityLocationId", "")) if customer.get("cityLocationId") else None,
                    "districtLocationId": str(customer.get("districtLocationId", "")) if customer.get("districtLocationId") else None,
                    "wardLocationId": str(customer.get("wardLocationId", "")) if customer.get("wardLocationId") else None,
                    "totalMoney": float(customer.get("totalMoney", 0)) if customer.get("totalMoney") else 0,
                    "startedDate": customer.get("startedDate") if customer.get("startedDate") else None,
                    "startedDepotId": str(customer.get("startedDepotId", "")),
                    "points": int(customer.get("points", 0)) if customer.get("points") else 0,
                    "totalBills": int(customer.get("totalBills", 0)) if customer.get("totalBills") else 0,
                    "lastBoughtDate": customer.get("lastBoughtDate") if customer.get("lastBoughtDate") else None,
                    "taxCode": str(customer.get("taxCode", "")) if customer.get("taxCode") else None,
                    "businessName": str(customer.get("businessName", "")) if customer.get("businessName") else None,
                    "businessAddress": str(customer.get("businessAddress", "")) if customer.get("businessAddress") else None,
                    "description": str(customer.get("description", "")) if customer.get("description") else None,
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                processed_customers.append(processed_customer)

            except Exception as e:
                logger.error(f"Lỗi khi xử lý khách hàng {customer.get('id', 'unknown')}: {str(e)}")
                continue
                
        return processed_customers

    def run_demo(self):
        """Chạy demo đồng bộ khách hàng"""
        try:
            # Lấy khách hàng từ ngày 1-1-2025 đến hiện tại
            end_date = datetime.now()
            # start_date = datetime(2024, 1, 1)
            start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Gọi API lấy dữ liệu và đồng bộ một lần duy nhất
            self.sync_customers(
                start_date=start_date,
                end_date=end_date,
                step_days=9,
                items_per_page=100
            )
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy demo: {str(e)}")
            raise

if __name__ == "__main__":
    # Chạy demo khi chạy file trực tiếp
    customer_service = CustomerService()
    customer_service.run_demo()
