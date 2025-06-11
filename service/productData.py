from typing import List, Dict, Any
from database.manager import DatabaseManager
import json
import uuid
import logging
from datetime import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductDataInserter:
    def __init__(self):
        """Khởi tạo class với DatabaseManager"""
        self.db = DatabaseManager()
        self.table_name = "products"

    def _prepare_value(self, value: Any) -> str:
        """
        Chuẩn bị giá trị để insert vào database
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, (dict, list)):
            return f"N'{json.dumps(value, ensure_ascii=False)}'"
        else:
            # Escape single quotes và chuyển đổi sang NVARCHAR
            escaped_value = str(value).replace("'", "''")
            return f"N'{escaped_value}'"

    def _prepare_bulk_values(self, items: List[Dict[str, Any]], columns: List[str]) -> List[str]:
        """
        Chuẩn bị danh sách giá trị cho bulk insert
        """
        values_list = []
        for item in items:
            values = []
            for col in columns:
                val = item.get(col)
                # Đảm bảo code không được null
                if col == 'code' and (val is None or val == ''):
                    val = f"PROD_{uuid.uuid4().hex[:8]}"
                values.append(self._prepare_value(val))
            values_list.append(f"({', '.join(values)})")
        return values_list

    def insert_or_update_products(self, data: List[Dict[str, Any]]):
        """
        Thêm hoặc cập nhật sản phẩm vào database
        
        Args:
            data: List các dictionary chứa dữ liệu sản phẩm
        """
        try:
            if not self.db.connect():
                logger.error("Không thể kết nối đến database")
                return

            if not data:
                logger.warning("Không có dữ liệu để xử lý")
                return

            # Đảm bảo mỗi sản phẩm có code
            for item in data:
                if not item.get('code'):
                    item['code'] = f"PROD_{uuid.uuid4().hex[:8]}"

            # Lấy danh sách cột từ dữ liệu đầu tiên
            columns = list(data[0].keys())
            values_list = self._prepare_bulk_values(data, columns)
            
            try:
                # Chia nhỏ bulk merge thành các batch
                batch_size = 1000
                total_processed = 0
                
                for i in range(0, len(values_list), batch_size):
                    batch_values = values_list[i:i + batch_size]
                    
                    # Tạo bảng tạm để chứa dữ liệu mới
                    temp_table = f"#temp_{self.table_name}"
                    create_temp_table_query = f"""
                    CREATE TABLE {temp_table} (
                        {', '.join([f'[{col}] NVARCHAR(MAX)' for col in columns])}
                    )
                    """
                    self.db.cursor.execute(create_temp_table_query)
                    
                    # Insert dữ liệu vào bảng tạm
                    insert_temp_query = f"""
                    INSERT INTO {temp_table} ({', '.join([f'[{col}]' for col in columns])})
                    VALUES {', '.join(batch_values)}
                    """
                    self.db.cursor.execute(insert_temp_query)
                    
                    # Thực hiện MERGE sử dụng code làm khóa
                    merge_query = f"""
                    MERGE {self.table_name} AS target
                    USING {temp_table} AS source
                    ON (target.code = source.code)
                    WHEN MATCHED THEN
                        UPDATE SET {', '.join([f'target.[{col}] = source.[{col}]' for col in columns if col != 'code'])}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join([f'[{col}]' for col in columns])})
                        VALUES ({', '.join([f'source.[{col}]' for col in columns])});
                    """
                    self.db.cursor.execute(merge_query)
                    
                    # Xóa bảng tạm
                    drop_temp_query = f"DROP TABLE {temp_table}"
                    self.db.cursor.execute(drop_temp_query)
                    
                    self.db.conn.commit()
                    total_processed += len(batch_values)
                    logger.info(f"Đã xử lý {total_processed}/{len(values_list)} sản phẩm")

                logger.info(f"Đã thêm/cập nhật thành công {total_processed} sản phẩm")
                
            except Exception as e:
                logger.error(f"Lỗi khi thêm/cập nhật sản phẩm: {str(e)}")
                logger.error(f"SQL Query: {insert_temp_query}")
                self.db.conn.rollback()
                return

        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu sản phẩm: {str(e)}")
            if self.db.conn:
                self.db.conn.rollback()
        finally:
            self.db.close()

class InventoryDataInserter:
    def __init__(self):
        """Khởi tạo class với DatabaseManager"""
        self.db = DatabaseManager()
        self.table_name = "product_inventory"

    def _prepare_value(self, value: Any) -> str:
        """
        Chuẩn bị giá trị để insert vào database
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, (dict, list)):
            return f"N'{json.dumps(value, ensure_ascii=False)}'"
        else:
            # Escape single quotes và chuyển đổi sang NVARCHAR
            escaped_value = str(value).replace("'", "''")
            return f"N'{escaped_value}'"

    def _prepare_bulk_values(self, items: List[Dict[str, Any]], columns: List[str]) -> List[str]:
        """
        Chuẩn bị danh sách giá trị cho bulk insert
        """
        values_list = []
        for item in items:
            values = []
            for col in columns:
                val = item.get(col)
                values.append(self._prepare_value(val))
            values_list.append(f"({', '.join(values)})")
        return values_list

    def insert_or_update_inventory(self, data: List[Dict[str, Any]]):
        """
        Thêm hoặc cập nhật dữ liệu inventory vào database
        
        Args:
            data: List các dictionary chứa dữ liệu inventory
        """
        try:
            if not self.db.connect():
                logger.error("Không thể kết nối đến database")
                return

            if not data:
                logger.warning("Không có dữ liệu inventory để xử lý")
                return

            # Lấy danh sách cột từ dữ liệu đầu tiên
            columns = list(data[0].keys())
            values_list = self._prepare_bulk_values(data, columns)
            
            try:
                # Chia nhỏ bulk merge thành các batch
                batch_size = 1000
                total_processed = 0
                
                for i in range(0, len(values_list), batch_size):
                    batch_values = values_list[i:i + batch_size]
                    
                    # Tạo bảng tạm để chứa dữ liệu mới
                    temp_table = f"#temp_{self.table_name}"
                    create_temp_table_query = f"""
                    CREATE TABLE {temp_table} (
                        {', '.join([f'[{col}] NVARCHAR(MAX)' for col in columns])}
                    )
                    """
                    self.db.cursor.execute(create_temp_table_query)
                    
                    # Insert dữ liệu vào bảng tạm
                    insert_temp_query = f"""
                    INSERT INTO {temp_table} ({', '.join([f'[{col}]' for col in columns])})
                    VALUES {', '.join(batch_values)}
                    """
                    self.db.cursor.execute(insert_temp_query)
                    
                    # Thực hiện MERGE sử dụng id làm khóa
                    merge_query = f"""
                    MERGE {self.table_name} AS target
                    USING {temp_table} AS source
                    ON (target.id = source.id)
                    WHEN MATCHED THEN
                        UPDATE SET {', '.join([f'target.[{col}] = source.[{col}]' for col in columns if col != 'id'])}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join([f'[{col}]' for col in columns])})
                        VALUES ({', '.join([f'source.[{col}]' for col in columns])});
                    """
                    self.db.cursor.execute(merge_query)
                    
                    # Xóa bảng tạm
                    drop_temp_query = f"DROP TABLE {temp_table}"
                    self.db.cursor.execute(drop_temp_query)
                    
                    self.db.conn.commit()
                    total_processed += len(batch_values)
                    logger.info(f"Đã xử lý {total_processed}/{len(values_list)} bản ghi inventory")

                logger.info(f"Đã thêm/cập nhật thành công {total_processed} bản ghi inventory")
                
            except Exception as e:
                logger.error(f"Lỗi khi thêm/cập nhật inventory: {str(e)}")
                logger.error(f"SQL Query: {insert_temp_query}")
                self.db.conn.rollback()
                return

        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu inventory: {str(e)}")
            if self.db.conn:
                self.db.conn.rollback()
        finally:
            self.db.close()

class AttributeDataInserter:
    """Class xử lý thêm/cập nhật dữ liệu attributes"""
    def __init__(self, db_manager=None):
        self.db = db_manager or DatabaseManager()
        self.table_name = "product_attributes"

    def _prepare_value(self, value):
        """Chuẩn bị giá trị cho câu lệnh SQL"""
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Escape dấu nháy đơn bằng cách thay thế bằng hai dấu nháy đơn
            escaped_value = value.replace("'", "''")
            return f"N'{escaped_value}'"
        elif isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            escaped_value = str(value).replace("'", "''")
            return f"N'{escaped_value}'"

    def _prepare_bulk_values(self, data_list):
        """Chuẩn bị danh sách giá trị cho bulk insert"""
        values = []
        for item in data_list:
            row_values = []
            for key in ["id", "fk_id", "attribute_id", "attribute_name", "name", "value", "display_order", "updated_at"]:
                value = item.get(key)
                row_values.append(self._prepare_value(value))
            values.append(f"({', '.join(row_values)})")
        return values

    def insert_or_update_attributes(self, data_list):
        """Thêm hoặc cập nhật dữ liệu attributes"""
        if not data_list:
            return

        try:
            if not self.db.connect():
                raise Exception("Không thể kết nối đến database")

            # Tạo bảng tạm
            temp_table_query = f"""
            CREATE TABLE #temp_{self.table_name} (
                id NVARCHAR(50),
                fk_id NVARCHAR(50),
                attribute_id NVARCHAR(50),
                attribute_name NVARCHAR(255),
                name NVARCHAR(255),
                value NVARCHAR(MAX),
                display_order INT,
                updated_at DATETIME
            )
            """
            self.db.cursor.execute(temp_table_query)

            # Chia nhỏ dữ liệu để tránh quá tải
            batch_size = 1000
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                values = self._prepare_bulk_values(batch)
                
                # Insert vào bảng tạm
                insert_query = f"""
                INSERT INTO #temp_{self.table_name} 
                ([id], [fk_id], [attribute_id], [attribute_name], [name], [value], [display_order], [updated_at])
                VALUES {', '.join(values)}
                """
                self.db.cursor.execute(insert_query)

            # Merge dữ liệu từ bảng tạm vào bảng chính
            merge_query = f"""
            MERGE {self.table_name} AS target
            USING #temp_{self.table_name} AS source
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.fk_id = source.fk_id,
                    target.attribute_id = source.attribute_id,
                    target.attribute_name = source.attribute_name,
                    target.name = source.name,
                    target.value = source.value,
                    target.display_order = source.display_order,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (id, fk_id, attribute_id, attribute_name, name, value, display_order, updated_at)
                VALUES (source.id, source.fk_id, source.attribute_id, source.attribute_name, 
                        source.name, source.value, source.display_order, source.updated_at);
            """
            self.db.cursor.execute(merge_query)
            self.db.conn.commit()

        except Exception as e:
            logger.error(f"Lỗi khi thêm/cập nhật attributes: {str(e)}")
            logger.error(f"SQL Query:\n{merge_query}")
            raise
        finally:
            self.db.close()

def process_product_data(data: List[Dict[str, Any]]):
    """
    Xử lý và lưu dữ liệu sản phẩm vào database
    
    Args:
        data: List các dictionary chứa dữ liệu sản phẩm
    """
    if not data:
        logger.warning("Không có dữ liệu sản phẩm để xử lý")
        return

    inserter = ProductDataInserter()
    inserter.insert_or_update_products(data)

def process_inventory_data(data: List[Dict[str, Any]]):
    """
    Xử lý và lưu dữ liệu inventory vào database
    
    Args:
        data: List các dictionary chứa dữ liệu inventory
    """
    if not data:
        logger.warning("Không có dữ liệu inventory để xử lý")
        return

    inserter = InventoryDataInserter()
    inserter.insert_or_update_inventory(data)

def process_attribute_data(data_list):
    """Hàm xử lý dữ liệu attributes"""
    try:
        inserter = AttributeDataInserter()
        inserter.insert_or_update_attributes(data_list)
        logger.info(f"Xử lý {len(data_list)} bản ghi attributes thành công")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý dữ liệu attributes: {str(e)}")
        raise 