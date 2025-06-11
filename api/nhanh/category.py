import requests
from typing import List, Dict, Any
import logging
from datetime import datetime
import sys
import os

# Thêm đường dẫn thư mục gốc vào PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database.manager import DatabaseManager

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CategoryService:
    def __init__(self):
        """Khởi tạo class với DatabaseManager"""
        self.db = DatabaseManager()
        self.api_url = "https://open.nhanh.vn/api/product/category"
        self.api_params = {
            "version": "2.0",
            "appId": "75541", 
            "businessId": "28099",
            "accessToken": "WMHCj89gaaCEHM1xjv7kcpPhsAhMJimNIvOOQPucPjb81ZC3IYM7GatYvXVVI2Q7hc8gvlTxJVAgkjsQO1u2BBGpVcWTwbMJUHGSGpq00GNKWp8DG9uqFIZphprqCeXEOWyeeIWTRPELhKPOiQ7EPEe2boMIIRDJMOH81kuVDI47zABD9sWOX9YisgqtxajjFa3Nv1flxtXUmrDk77Wum9RPyHVBXoNVLGwDAmsSzdEkXogirUoyXRY4gkQO"
        }
        self.max_level = 0  # Để theo dõi cấp độ sâu nhất của categories
        self.total_categories = 0  # Để đếm tổng số categories
        self.root_categories = []  # Lưu danh sách categories gốc
        self.level_categories = {}  # Lưu categories theo từng cấp độ

    def get_table_name(self, level: int) -> str:
        """Lấy tên bảng dựa vào cấp độ"""
        if level == 0:
            return "categories"
        return f"categories_level_{level}"

    def create_table_for_level(self, level: int):
        """Tạo bảng cho một cấp độ cụ thể"""
        table_name = self.get_table_name(level)
        
        # Cấu trúc cột cơ bản cho mỗi bảng
        columns = {
            "id": "INT PRIMARY KEY",
            "parent_id": "INT",
            "name": "NVARCHAR(255)",
            "code": "NVARCHAR(255)",
            "order_num": "INT",
            "show_home": "INT",
            "show_home_order": "INT", 
            "private_id": "INT",
            "status": "INT",
            "image": "NVARCHAR(MAX)",
            "content": "NVARCHAR(MAX)",
            "level": "INT",  # Thêm cột level để dễ dàng truy vấn
            "created_at": "DATETIME DEFAULT GETDATE()",
            "updated_at": "DATETIME DEFAULT GETDATE()"
        }
        
        try:
            if not self.db.connect():
                raise Exception("Không thể kết nối đến database")
                
            # Tạo bảng
            if not self.db.create_table(table_name, columns):
                raise Exception(f"Không thể tạo bảng {table_name}")
                
            # Thêm foreign key constraint nếu không phải bảng gốc
            if level > 0:
                parent_table = self.get_table_name(level - 1)
                fk_sql = f"""
                IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_{table_name}_parent_id')
                BEGIN
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT FK_{table_name}_parent_id 
                    FOREIGN KEY (parent_id) REFERENCES {parent_table}(id) ON DELETE CASCADE
                END
                """
                self.db.cursor.execute(fk_sql)
                self.db.conn.commit()
                
            logger.info(f"Tạo bảng {table_name} thành công")
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo bảng {table_name}: {str(e)}")
            raise
        finally:
            self.db.close()

    def collect_categories_by_level(self, categories: List[Dict], current_level: int = 0, parent_id: int = 0):
        """
        Thu thập categories theo từng cấp độ
        
        Args:
            categories: List categories
            current_level: Cấp độ hiện tại
            parent_id: ID của category cha
        """
        if current_level == 0:
            # Lưu categories gốc
            self.root_categories = categories
            self.level_categories[0] = categories
            logger.info(f"Phát hiện {len(categories)} categories gốc")
        else:
            # Lưu categories của cấp độ hiện tại
            if current_level not in self.level_categories:
                self.level_categories[current_level] = []
            self.level_categories[current_level].extend(categories)
            
        self.max_level = max(self.max_level, current_level)
        self.total_categories += len(categories)
        
        # Đệ quy xử lý các categories con
        for cat in categories:
            if "childs" in cat and cat["childs"]:
                self.collect_categories_by_level(cat["childs"], current_level + 1, cat["id"])

    def create_all_tables(self):
        """Tạo tất cả các bảng cần thiết dựa vào cấu trúc categories"""
        logger.info(f"Phát hiện {self.total_categories} categories với {self.max_level + 1} cấp độ")
        
        # Tạo các bảng cho từng cấp độ
        for level in range(self.max_level + 1):
            self.create_table_for_level(level)

    def get_categories(self) -> List[Dict[str, Any]]:
        """Lấy dữ liệu categories từ API Nhanh.vn"""
        try:
            response = requests.post(self.api_url, data=self.api_params)
            response.raise_for_status()
            data = response.json()
            
            if data.get("code") != 1:
                raise Exception(f"API trả về lỗi: {data}")
                
            categories = data.get("data", [])
            logger.info(f"Lấy được {len(categories)} categories từ API")
            return categories
            
        except Exception as e:
            logger.error(f"Lỗi khi gọi API: {str(e)}")
            raise

    def sync_categories_by_level(self, level: int):
        """
        Đồng bộ categories của một cấp độ cụ thể vào database
        
        Args:
            level: Cấp độ cần đồng bộ
        """
        if level not in self.level_categories:
            return
            
        table_name = self.get_table_name(level)
        categories = self.level_categories[level]
        
        try:
            if not self.db.connect():
                raise Exception("Không thể kết nối đến database")
                
            try:
                # Xóa dữ liệu cũ
                self.db.cursor.execute(f"DELETE FROM {table_name}")
                
                # Insert dữ liệu mới
                for cat in categories:
                    # Tìm parent_id
                    parent_id = 0
                    if level > 0:
                        # Tìm trong cấp độ trước đó
                        for parent_cat in self.level_categories[level - 1]:
                            if "childs" in parent_cat and any(child["id"] == cat["id"] for child in parent_cat["childs"]):
                                parent_id = parent_cat["id"]
                                break
                    
                    category = {
                        "id": cat["id"],
                        "parent_id": parent_id,
                        "name": cat["name"],
                        "code": cat["code"],
                        "order_num": cat["order"],
                        "show_home": cat["showHome"],
                        "show_home_order": cat["showHomeOrder"],
                        "private_id": cat["privateId"],
                        "status": cat["status"],
                        "image": cat["image"],
                        "content": cat.get("content", ""),
                        "level": level,
                        "updated_at": datetime.now()
                    }
                    
                    if not self.db.insert_data(table_name, category):
                        raise Exception(f"Lỗi khi insert category {cat['id']} vào {table_name}")
                
                self.db.conn.commit()
                logger.info(f"Đồng bộ thành công {len(categories)} categories vào bảng {table_name}")
                
            finally:
                self.db.close()
                
        except Exception as e:
            logger.error(f"Lỗi khi đồng bộ dữ liệu vào {table_name}: {str(e)}")
            raise

    def verify_sync(self):
        """Kiểm tra xem dữ liệu đã được đồng bộ đầy đủ chưa"""
        try:
            if not self.db.connect():
                raise Exception("Không thể kết nối đến database")
                
            try:
                total_synced = 0
                
                # Kiểm tra từng cấp độ
                for level in range(self.max_level + 1):
                    table_name = self.get_table_name(level)
                    self.db.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                    count = self.db.cursor.fetchone()[0]
                    expected_count = len(self.level_categories.get(level, []))
                    
                    logger.info(f"Bảng {table_name}: {count} categories (mong đợi: {expected_count})")
                    
                    if count != expected_count:
                        logger.warning(f"Số lượng categories không khớp ở cấp độ {level}! Đã đồng bộ: {count}, Mong đợi: {expected_count}")
                    
                    total_synced += count
                
                logger.info(f"Tổng số categories đã đồng bộ: {total_synced}")
                logger.info(f"Tổng số categories từ API: {self.total_categories}")
                
                if total_synced != self.total_categories:
                    logger.warning(f"Số lượng categories không khớp! Đã đồng bộ: {total_synced}, Từ API: {self.total_categories}")
                else:
                    logger.info("Đồng bộ dữ liệu thành công và đầy đủ!")
                    
            finally:
                self.db.close()
                
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra đồng bộ: {str(e)}")
            raise

    def sync_categories(self):
        """Đồng bộ dữ liệu categories từ Nhanh.vn vào database"""
        try:
            # Reset counters và collections
            self.max_level = 0
            self.total_categories = 0
            self.root_categories = []
            self.level_categories = {}
            
            # Lấy dữ liệu từ API
            categories = self.get_categories()
            
            # Phân tích và thu thập categories theo cấp độ
            self.collect_categories_by_level(categories)
            
            # Tạo tất cả các bảng cần thiết
            self.create_all_tables()
            
            # Đồng bộ từng cấp độ
            for level in range(self.max_level + 1):
                self.sync_categories_by_level(level)
            
            # Kiểm tra kết quả đồng bộ
            self.verify_sync()
            
        except Exception as e:
            logger.error(f"Lỗi khi đồng bộ categories: {str(e)}")
            raise

    def run_demo(self):
        """Chạy demo đồng bộ categories"""
        try:
            logger.info("Bắt đầu đồng bộ categories...")
            self.sync_categories()
            logger.info("Đồng bộ categories hoàn tất!")
        except Exception as e:
            logger.error(f"Lỗi khi chạy demo: {str(e)}")
            raise

