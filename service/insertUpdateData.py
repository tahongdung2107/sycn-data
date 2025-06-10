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
                    values.append(f"N'{str(uuid.uuid4())}'")
                else:
                    values.append(self._prepare_value(item.get(col)))
            values_list.append(f"({', '.join(values)})")
        return values_list

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

        # Xử lý dữ liệu và tạo câu lệnh bulk insert
        for parent_id, data in zip(parent_ids, data_list):
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        values = []
                        for col in all_columns:
                            if col == 'id':
                                values.append(f"N'{str(uuid.uuid4())}'")
                            elif col == 'fk_id':
                                values.append(f"N'{parent_id}'")
                            else:
                                values.append(self._prepare_value(item.get(col)))
                        all_values.append(f"({', '.join(values)})")
            elif isinstance(data, dict):
                values = []
                for col in all_columns:
                    if col == 'id':
                        values.append(f"N'{str(uuid.uuid4())}'")
                    elif col == 'fk_id':
                        values.append(f"N'{parent_id}'")
                    else:
                        values.append(self._prepare_value(data.get(col)))
                all_values.append(f"({', '.join(values)})")

        if all_values:
            try:
                if self.db_manager.connect():
                    # Chia nhỏ bulk insert thành các batch để tránh quá tải
                    batch_size = 1000
                    for i in range(0, len(all_values), batch_size):
                        batch_values = all_values[i:i + batch_size]
                        query = f"""
                        INSERT INTO {child_table} ({', '.join(all_columns)})
                        VALUES {', '.join(batch_values)}
                        """
                        self.db_manager.cursor.execute(query)
                        self.db_manager.conn.commit()
            except Exception as e:
                print(f"Lỗi khi thêm dữ liệu vào bảng con {child_table}: {str(e)}")
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
                
                # Lưu ID của record
                if 'id' in regular_data:
                    parent_ids.append(regular_data['id'])
                else:
                    # Tạo ID mới nếu không có
                    new_id = str(uuid.uuid4())
                    regular_data['id'] = new_id
                    parent_ids.append(new_id)

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

def process_order_data(data: List[Dict[str, Any]], table_name: str = 'orders'):
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
