from typing import List, Dict, Any
from database.manager import DatabaseManager
import json
import uuid

class DataInserter:
    def __init__(self):
        self.db_manager = DatabaseManager()

    def _prepare_value(self, value: Any) -> str:
        """
        Chuẩn bị giá trị để insert vào database
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return '1' if value else '0'
        elif isinstance(value, (dict, list)):
            return f"'{json.dumps(value, ensure_ascii=False)}'"
        else:
            # Escape single quotes in string values and handle special characters
            escaped_value = str(value).replace("'", "''")
            # Convert to NVARCHAR compatible string
            return f"N'{escaped_value}'"

    def _prepare_bulk_values(self, items: List[Dict[str, Any]], columns: List[str]) -> List[str]:
        """
        Chuẩn bị danh sách giá trị cho bulk insert
        """
        values_list = []
        for item in items:
            values = []
            for col in columns:
                if col == 'id':
                    # Ưu tiên sử dụng Id từ data nếu có
                    item_id = item.get('Id') or item.get('id') or str(uuid.uuid4())
                    values.append(f"N'{item_id}'")
                else:
                    val = item.get(col)
                    if val is None:
                        values.append('NULL')
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    elif isinstance(val, bool):
                        values.append('1' if val else '0')
                    elif isinstance(val, (dict, list)):
                        values.append(f"'{json.dumps(val, ensure_ascii=False)}'")
                    else:
                        escaped_value = str(val).replace("'", "''")
                        values.append(f"N'{escaped_value}'")
            values_list.append(f"({', '.join(values)})")
        return values_list

    def _check_and_create_table(self, table_name: str, columns: List[str]):
        """
        Kiểm tra và tạo bảng nếu chưa tồn tại
        """
        try:
            if self.db_manager.connect():
                # Kiểm tra bảng có tồn tại không
                check_table_query = f"""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
                BEGIN
                    CREATE TABLE {table_name} (
                        id NVARCHAR(50) PRIMARY KEY,
                        fk_id NVARCHAR(50),
                        {', '.join([f'[{col}] NVARCHAR(MAX)' for col in columns if col not in ['id', 'fk_id']])}
                    )
                END
                """
                self.db_manager.cursor.execute(check_table_query)
                self.db_manager.conn.commit()
        except Exception as e:
            print(f"Lỗi khi kiểm tra/tạo bảng {table_name}: {str(e)}")
            self.db_manager.conn.rollback()

    def _bulk_insert_nested_data(self, parent_ids: List[str], field_name: str, data_list: List[Any], table_name: str):
        """
        Thêm dữ liệu vào bảng con theo bulk
        """
        if not data_list:
            return

        child_table = f"{table_name}_{field_name}"
        all_values = []
        all_columns = set()

        # Thu thập tất cả các cột từ dữ liệu
        for data in data_list:
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        all_columns.update(item.keys())
            elif isinstance(data, dict):
                all_columns.update(data.keys())

        # Thêm cột id và fk_id
        all_columns = ['id', 'fk_id'] + list(all_columns - {'id'})

        # Kiểm tra và tạo bảng nếu chưa tồn tại
        self._check_and_create_table(child_table, all_columns)

        # Xử lý dữ liệu và tạo câu lệnh bulk insert
        for parent_id, data in zip(parent_ids, data_list):
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        values = []
                        for col in all_columns:
                            if col == 'id':
                                # Ưu tiên sử dụng Id từ data nếu có
                                item_id = item.get('Id') or item.get('id') or str(uuid.uuid4())
                                values.append(f"N'{item_id}'")
                            elif col == 'fk_id':
                                values.append(f"N'{parent_id}'")
                            else:
                                val = item.get(col)
                                if val is None:
                                    values.append('NULL')
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                elif isinstance(val, bool):
                                    values.append('1' if val else '0')
                                elif isinstance(val, (dict, list)):
                                    values.append(f"'{json.dumps(val, ensure_ascii=False)}'")
                                else:
                                    escaped_value = str(val).replace("'", "''")
                                    values.append(f"N'{escaped_value}'")
                        all_values.append(f"({', '.join(values)})")
            elif isinstance(data, dict):
                values = []
                for col in all_columns:
                    if col == 'id':
                        # Ưu tiên sử dụng Id từ data nếu có
                        item_id = data.get('Id') or data.get('id') or str(uuid.uuid4())
                        values.append(f"N'{item_id}'")
                    elif col == 'fk_id':
                        values.append(f"N'{parent_id}'")
                    else:
                        val = data.get(col)
                        if val is None:
                            values.append('NULL')
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        elif isinstance(val, bool):
                            values.append('1' if val else '0')
                        elif isinstance(val, (dict, list)):
                            values.append(f"'{json.dumps(val, ensure_ascii=False)}'")
                        else:
                            escaped_value = str(val).replace("'", "''")
                            values.append(f"N'{escaped_value}'")
                all_values.append(f"({', '.join(values)})")

        if all_values:
            try:
                if self.db_manager.connect():
                    # Chia nhỏ bulk insert thành các batch để tránh quá tải
                    batch_size = 1000
                    for i in range(0, len(all_values), batch_size):
                        batch_values = all_values[i:i + batch_size]
                        # Bọc tất cả tên cột trong dấu ngoặc vuông
                        columns = [f"[{col}]" for col in all_columns]
                        query = f"""
                        INSERT INTO {child_table} ({', '.join(columns)})
                        VALUES {', '.join(batch_values)}
                        """
                        self.db_manager.cursor.execute(query)
                        self.db_manager.conn.commit()
            except Exception as e:
                print(f"Lỗi khi thêm dữ liệu vào bảng con {child_table}: {str(e)}")
                print(f"SQL Query: {query}")  # In ra câu lệnh SQL khi có lỗi
                self.db_manager.conn.rollback()

    def insert_or_update_data(self, data: List[Dict[str, Any]], table_name: str):
        """
        Thêm hoặc cập nhật dữ liệu vào bảng theo bulk
        
        Args:
            data: List các dictionary chứa dữ liệu
            table_name: Tên bảng
        """
        try:
            if not self.db_manager.connect():
                print("Không thể kết nối đến database")
                return

            # Tách dữ liệu thường và dữ liệu lồng nhau
            regular_data_list = []
            nested_data_dict = {}
            parent_ids = []

            for item in data:
                regular_data = {}
                nested_data = {}
                
                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        nested_data[key] = value
                    else:
                        regular_data[key] = value

                regular_data_list.append(regular_data)
                
                # Lưu ID của record, ưu tiên sử dụng Id từ data
                item_id = item.get('Id') or item.get('id') or str(uuid.uuid4())
                if 'id' not in regular_data:
                    regular_data['id'] = item_id
                parent_ids.append(item_id)

                # Thu thập dữ liệu lồng nhau
                for field_name, nested_value in nested_data.items():
                    if field_name not in nested_data_dict:
                        nested_data_dict[field_name] = []
                    nested_data_dict[field_name].append(nested_value)

            # Thực hiện bulk insert cho dữ liệu thường
            if regular_data_list:
                columns = list(regular_data_list[0].keys())
                values_list = self._prepare_bulk_values(regular_data_list, columns)
                
                try:
                    # Chia nhỏ bulk insert thành các batch
                    batch_size = 1000
                    for i in range(0, len(values_list), batch_size):
                        batch_values = values_list[i:i + batch_size]
                        insert_query = f"""
                        INSERT INTO {table_name} ({', '.join(columns)})
                        VALUES {', '.join(batch_values)}
                        """
                        self.db_manager.cursor.execute(insert_query)
                        self.db_manager.conn.commit()
                except Exception as e:
                    print(f"Lỗi khi thêm dữ liệu vào bảng {table_name}: {str(e)}")
                    self.db_manager.conn.rollback()
                    return

            # Xử lý dữ liệu lồng nhau theo bulk
            for field_name, nested_data_list in nested_data_dict.items():
                self._bulk_insert_nested_data(parent_ids, field_name, nested_data_list, table_name)

            print(f"Đã thêm {len(regular_data_list)} bản ghi vào bảng {table_name}")

        except Exception as e:
            print(f"Lỗi khi thêm/cập nhật dữ liệu: {str(e)}")
            if self.db_manager.conn:
                self.db_manager.conn.rollback()
        finally:
            self.db_manager.close()

def process_data(data: List[Dict[str, Any]], table_name: str = 'orders'):
    """
    Xử lý dữ liệu đơn hàng và thêm vào database
    
    Args:
        data: List các dictionary chứa dữ liệu đơn hàng
        table_name: Tên bảng để lưu dữ liệu (mặc định là 'orders')
    """
    if not data:
        print("Không có dữ liệu để xử lý")
        return

    inserter = DataInserter()
    inserter.insert_or_update_data(data, table_name)
