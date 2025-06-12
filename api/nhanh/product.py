import requests
from typing import List, Dict, Any, Tuple
import logging
from datetime import datetime
import sys
import os
import json
import uuid
import traceback
from service.fetchData import NhanhAPIClient
from service.createTable import create_table
from service.productData import process_product_data, process_inventory_data, process_attribute_data, process_inventory_depot_data

# Thêm đường dẫn thư mục gốc vào PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database.manager import DatabaseManager

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductService:
    def __init__(self):
        """Khởi tạo class với DatabaseManager và NhanhAPIClient"""
        self.db = DatabaseManager()
        self.api_client = NhanhAPIClient()
        self.table_name = "products"

    def create_table(self):
        """Tạo bảng products, inventory và attributes với cấu trúc phù hợp với dữ liệu từ API"""
        # Cấu trúc bảng products
        product_columns = {
            "code": "NVARCHAR(255) PRIMARY KEY",
            "idNhanh": "NVARCHAR(50)",
            "name": "NVARCHAR(MAX)",
            "parent_id": "NVARCHAR(50)",
            "category_id": "NVARCHAR(50)",
            "category_name": "NVARCHAR(255)",
            "price": "NVARCHAR(50)",
            "old_price": "NVARCHAR(50)",
            "wholesale_price": "NVARCHAR(50)",
            "inventory_status": "NVARCHAR(50)",
            "status": "NVARCHAR(50)",
            "show_home": "NVARCHAR(50)",
            "show_home_order": "NVARCHAR(50)",
            "image": "NVARCHAR(MAX)",
            "images": "NVARCHAR(MAX)",
            "content": "NVARCHAR(MAX)",
            "description": "NVARCHAR(MAX)",
            "created_at": "DATETIME DEFAULT GETDATE()",
            "updated_at": "DATETIME DEFAULT GETDATE()"
        }

        # Cấu trúc bảng inventory (chỉ chứa tồn kho tổng)
        inventory_columns = {
            "id": "NVARCHAR(50) PRIMARY KEY",
            "fk_id": "NVARCHAR(50)",
            "remain": "INT",
            "shipping": "INT",
            "damaged": "INT",
            "holding": "INT",
            "warranty": "INT",
            "warranty_holding": "INT",
            "transfering": "INT",
            "available": "INT",
            "created_at": "DATETIME DEFAULT GETDATE()",
            "updated_at": "DATETIME DEFAULT GETDATE()"
        }

        # Cấu trúc bảng inventory_depot (chứa tồn kho theo kho)
        inventory_depot_columns = {
            "id": "NVARCHAR(50) PRIMARY KEY",
            "fk_id": "NVARCHAR(50)",  # Liên kết với products.idNhanh
            "depot_id": "NVARCHAR(50)",
            "remain": "INT",
            "shipping": "INT",
            "damaged": "INT",
            "holding": "INT",
            "warranty": "INT",
            "warranty_holding": "INT",
            "transfering": "INT",
            "available": "INT",
            "created_at": "DATETIME DEFAULT GETDATE()",
            "updated_at": "DATETIME DEFAULT GETDATE()"
        }

        # Cấu trúc bảng attributes
        attribute_columns = {
            "id": "INT IDENTITY(1,1) PRIMARY KEY",
            "fk_id": "NVARCHAR(50)",  # Liên kết với products.idNhanh
            "attribute_id": "NVARCHAR(50)",  # ID của thuộc tính (ví dụ: 86658)
            "attribute_name": "NVARCHAR(255)",  # Tên thuộc tính (ví dụ: Kích cỡ)
            "name": "NVARCHAR(255)",  # Giá trị thuộc tính (ví dụ: S)
            "value": "NVARCHAR(MAX)",  # Giá trị bổ sung (nếu có)
            "display_order": "INT",  # Đổi tên từ order thành display_order
            "created_at": "DATETIME DEFAULT GETDATE()",
            "updated_at": "DATETIME DEFAULT GETDATE()"
        }
        
        try:
            if not self.db.connect():
                raise Exception("Không thể kết nối đến database")
                
            # Xóa bảng cũ nếu tồn tại
            drop_tables_query = f"""
            IF OBJECT_ID('product_attributes', 'U') IS NOT NULL DROP TABLE product_attributes;
            IF OBJECT_ID('product_inventory_depot', 'U') IS NOT NULL DROP TABLE product_inventory_depot;
            IF OBJECT_ID('product_inventory', 'U') IS NOT NULL DROP TABLE product_inventory;
            IF OBJECT_ID('{self.table_name}', 'U') IS NOT NULL DROP TABLE {self.table_name};
            """
            self.db.cursor.execute(drop_tables_query)
            self.db.conn.commit()
                
            # Tạo bảng products
            if not self.db.create_table(self.table_name, product_columns):
                raise Exception(f"Không thể tạo bảng {self.table_name}")
            
            # Tạo bảng inventory
            if not self.db.create_table("product_inventory", inventory_columns):
                raise Exception("Không thể tạo bảng product_inventory")

            # Tạo bảng inventory_depot
            if not self.db.create_table("product_inventory_depot", inventory_depot_columns):
                raise Exception("Không thể tạo bảng product_inventory_depot")

            # Tạo bảng attributes
            if not self.db.create_table("product_attributes", attribute_columns):
                raise Exception("Không thể tạo bảng product_attributes")
            
            logger.info("Tạo bảng products, product_inventory, product_inventory_depot và product_attributes thành công")
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo bảng: {str(e)}")
            raise
        finally:
            self.db.close()

    def get_products(self, start_date: datetime = None, end_date: datetime = None, 
                    params: Dict = None, step_days: int = None, items_per_page: int = 100,
                    date_from_field: str = 'updatedDateTimeFrom', 
                    date_to_field: str = 'updatedDateTimeTo',
                    data_key: str = 'products') -> Dict[str, Any]:
        """
        Lấy dữ liệu sản phẩm từ API Nhanh.vn theo khoảng thời gian
        """
        # Nếu không có ngày bắt đầu và kết thúc, lấy tất cả sản phẩm
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
                path='/product/search',
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
                # Chuyển đổi list thành dictionary với key là idNhanh
                products_dict = {}
                for product in result:
                    if isinstance(product, dict) and 'idNhanh' in product:
                        products_dict[str(product['idNhanh'])] = product
                return {'data': {'products': products_dict}}
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi gọi API: {str(e)}")
            raise

    def sync_products(self, start_date: datetime = None, end_date: datetime = None,
                     step_days: int = None, items_per_page: int = 100):
        """
        Đồng bộ sản phẩm từ Nhanh.vn vào database theo khoảng thời gian
        """
        try:
            # Tạo bảng nếu chưa tồn tại
            self.create_table()
            
            # Lấy dữ liệu từ API
            result = self.get_products(
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
                
            if data:
                # Chuyển đổi list thành dictionary với key là idNhanh
                products_dict = {}
                for product in data:
                    if isinstance(product, dict) and 'idNhanh' in product:
                        products_dict[str(product['idNhanh'])] = product
                
                logger.info(f"Tổng số sản phẩm nhận được từ API: {len(data)}")
                logger.info(f"Số sản phẩm sau khi chuyển đổi thành dictionary: {len(products_dict)}")
                
                if products_dict:
                    # Xử lý dữ liệu trước khi lưu vào database
                    processed_products, inventory_data, inventory_depot_data, attribute_data = self.process_product_data(products_dict)
                    
                    logger.info(f"Số sản phẩm sau khi xử lý: {len(processed_products)}")
                    logger.info(f"Số bản ghi inventory: {len(inventory_data)}")
                    logger.info(f"Số bản ghi inventory_depot: {len(inventory_depot_data)}")
                    logger.info(f"Số bản ghi attributes: {len(attribute_data)}")
                    
                    if processed_products:
                        try:
                            # Kiểm tra số lượng sản phẩm trùng lặp
                            product_codes = set()
                            duplicate_codes = set()
                            for product in processed_products:
                                code = product.get('code')
                                if code in product_codes:
                                    duplicate_codes.add(code)
                                product_codes.add(code)
                            
                            if duplicate_codes:
                                logger.warning(f"Phát hiện {len(duplicate_codes)} mã sản phẩm trùng lặp")
                                logger.warning(f"Danh sách mã trùng: {list(duplicate_codes)[:10]}...")
                            
                            process_product_data(processed_products)
                            process_inventory_data(inventory_data)
                            process_inventory_depot_data(inventory_depot_data)
                            process_attribute_data(attribute_data)
                        except Exception as e:
                            logger.error(f"Lỗi khi lưu dữ liệu: {str(e)}")
                            raise
                else:
                    logger.warning("Không có sản phẩm hợp lệ để xử lý")
            else:
                logger.warning("Không có dữ liệu sản phẩm để xử lý")
                
        except Exception as e:
            logger.error(f"Lỗi khi đồng bộ sản phẩm: {str(e)}")
            raise

    def process_product_data(self, products_data: Dict[str, Dict]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Xử lý dữ liệu sản phẩm trước khi lưu vào database
        
        Args:
            products_data: Dictionary chứa dữ liệu sản phẩm với key là idNhanh
        Returns:
            Tuple chứa (processed_products, inventory_data, inventory_depot_data, attribute_data)
        """
        processed_products = []
        inventory_data = []
        inventory_depot_data = []
        attribute_data = []
        
        for product_id, product in products_data.items():
            try:
                # Đảm bảo product là dictionary
                if isinstance(product, str):
                    product = json.loads(product)
                elif not isinstance(product, dict):
                    logger.warning(f"Bỏ qua sản phẩm không phải dictionary: {product}")
                    continue

                # Xử lý dữ liệu sản phẩm
                processed_product = {
                    "code": str(product.get("code", "")),
                    "idNhanh": str(product_id),
                    "name": str(product.get("name", "")),
                    "parent_id": str(product.get("parentId", "")),
                    "category_id": str(product.get("categoryId", "")),
                    "category_name": str(product.get("categoryName", "")),
                    "price": str(product.get("price", "0")),
                    "old_price": str(product.get("oldPrice", "0")),
                    "wholesale_price": str(product.get("wholesalePrice", "0")),
                    "inventory_status": str(product.get("inventoryStatus", "0")),
                    "status": str(product.get("status", "0")),
                    "show_home": str(product.get("showHome", "0")),
                    "show_home_order": str(product.get("showHomeOrder", "0")),
                    "image": str(product.get("image", "")),
                    "images": json.dumps(product.get("images", [])),
                    "content": str(product.get("content", "")),
                    "description": str(product.get("description", "")),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                processed_products.append(processed_product)

                # Xử lý attributes - Bỏ trường id vì sẽ tự động tăng
                attributes = product.get("attributes", [])
                if isinstance(attributes, list):
                    for attr_group in attributes:
                        if isinstance(attr_group, dict):
                            for attr_id, attr_data in attr_group.items():
                                if isinstance(attr_data, dict):
                                    attribute = {
                                        "fk_id": str(product_id),
                                        "attribute_id": str(attr_id),
                                        "attribute_name": str(attr_data.get("attributeName", "")),
                                        "name": str(attr_data.get("name", "")),
                                        "value": str(attr_data.get("value", "")),
                                        "display_order": int(attr_data.get("order", 0)),
                                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    }
                                    attribute_data.append(attribute)

                # Xử lý dữ liệu inventory
                inventory = product.get("inventory", {})
                if isinstance(inventory, str):
                    inventory = json.loads(inventory)
                
                # Xử lý inventory chính
                main_inventory = {
                    "id": f"INV_{uuid.uuid4().hex[:8]}",
                    "fk_id": str(product_id),
                    "remain": int(inventory.get("remain", 0)),
                    "shipping": int(inventory.get("shipping", 0)),
                    "damaged": int(inventory.get("damaged", 0)),
                    "holding": int(inventory.get("holding", 0)),
                    "warranty": int(inventory.get("warranty", 0)),
                    "warranty_holding": int(inventory.get("warrantyHolding", 0)),
                    "transfering": int(inventory.get("transfering", 0)),
                    "available": int(inventory.get("available", 0)),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                inventory_data.append(main_inventory)

                # Xử lý inventory theo kho
                depots = inventory.get("depots", {})
                if isinstance(depots, dict):
                    for depot_id, depot_data in depots.items():
                        depot_inventory = {
                            "id": f"INV_DEPOT_{uuid.uuid4().hex[:8]}",
                            "fk_id": str(product_id),
                            "depot_id": str(depot_id),
                            "remain": int(depot_data.get("remain", 0)),
                            "shipping": int(depot_data.get("shipping", 0)),
                            "damaged": int(depot_data.get("damaged", 0)),
                            "holding": int(depot_data.get("holding", 0)),
                            "warranty": int(depot_data.get("warranty", 0)),
                            "warranty_holding": int(depot_data.get("warrantyHolding", 0)),
                            "transfering": int(depot_data.get("transfering", 0)),
                            "available": int(depot_data.get("available", 0)),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        inventory_depot_data.append(depot_inventory)

            except Exception as e:
                logger.error(f"Lỗi khi xử lý sản phẩm {product_id}: {str(e)}")
                continue
                
        return processed_products, inventory_data, inventory_depot_data, attribute_data

    def run_demo(self):
        """Chạy demo đồng bộ sản phẩm"""
        try:
            # Lấy sản phẩm từ ngày 1-1-2025 đến 1-3-2025
            end_date = datetime.now()
            start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Gọi API lấy dữ liệu và đồng bộ một lần duy nhất
            self.sync_products(
                start_date=start_date,
                end_date=end_date,
                step_days=9,
                items_per_page=100
            )
            
        except Exception as e:
            logger.error(f"Lỗi khi chạy demo: {str(e)}")
            raise
