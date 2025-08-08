from database.manager import DatabaseManagerCRM
import json
from typing import Dict, Any, List
import time

def escape_column_name(column_name: str) -> str:
    reserved_keywords = {
        'order', 'group', 'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'outer',
        'having', 'by', 'desc', 'asc', 'top', 'distinct', 'union', 'all', 'insert', 'update',
        'delete', 'create', 'drop', 'alter', 'table', 'view', 'index', 'primary', 'foreign',
        'key', 'references', 'constraint', 'default', 'check', 'unique', 'null', 'not', 'and',
        'or', 'in', 'exists', 'between', 'like', 'is', 'as', 'on', 'using', 'natural', 'cross'
    }
    return f'[{column_name}]' if column_name.lower() in reserved_keywords else f'[{column_name}]'

def get_sqlite_type(value: Any) -> str:
    return 'NVARCHAR(MAX)'

def ensure_table_exists(db_manager, table, obj, parent=False):
    check_sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table}'"
    db_manager.cursor.execute(check_sql)
    if not db_manager.cursor.fetchone():
        columns = []
        for k, v in obj.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                continue
            elif isinstance(v, dict):
                columns.append(f'{escape_column_name(k)} {get_sqlite_type(v)}')
            elif k == 'id':
                columns.append(f'{escape_column_name(k)} NVARCHAR(255) PRIMARY KEY')
            else:
                columns.append(f'{escape_column_name(k)} {get_sqlite_type(v)}')
        if parent:
            columns.append(f'{escape_column_name("fk_id")} NVARCHAR(255)')
        col_str = ', '.join(columns)
        sql = f'CREATE TABLE {escape_column_name(table)} ({col_str})'
        db_manager.cursor.execute(sql)

def ensure_columns_exist(db_manager, table, obj, parent=False):
    db_manager.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table}'")
    existing_cols = set([row[0] for row in db_manager.cursor.fetchall()])
    for k, v in obj.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            continue
        if k not in existing_cols:
            try:
                db_manager.cursor.execute(f'ALTER TABLE {escape_column_name(table)} ADD {escape_column_name(k)} {get_sqlite_type(v)}')
                existing_cols.add(k)
            except Exception as e:
                print(f"  Warning: Không thể thêm column {k}: {e}")
    if parent and 'fk_id' not in existing_cols:
        try:
            db_manager.cursor.execute(f'ALTER TABLE {escape_column_name(table)} ADD {escape_column_name("fk_id")} NVARCHAR(255)')
            existing_cols.add('fk_id')
        except Exception as e:
            print(f"  Warning: Không thể thêm column fk_id: {e}")

def prepare_main_table_data(obj: Dict[str, Any]) -> tuple:
    """Chuẩn bị dữ liệu cho bảng chính, trả về fields và nested data"""
    fields = {}
    nested = {}
    
    for k, v in obj.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            nested[k] = v
        elif isinstance(v, (dict, list)):
            fields[k] = json.dumps(v, ensure_ascii=False)
        else:
            fields[k] = v
    
    return fields, nested

def insert_main_table_batch(db_manager, table_name: str, records: List[Dict[str, Any]]):
    """Insert batch records vào bảng chính"""
    if not records:
        return
    
    # Đảm bảo tất cả columns tồn tại trước khi insert
    for obj in records:
        fields, _ = prepare_main_table_data(obj)
        ensure_table_exists(db_manager, table_name, obj, parent=False)
        ensure_columns_exist(db_manager, table_name, obj, parent=False)
    
    # Xử lý batch insert/update
    for obj in records:
        fields, _ = prepare_main_table_data(obj)
        
        # Chỉ lấy các columns có trong bảng
        db_manager.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}'")
        existing_cols = set([row[0] for row in db_manager.cursor.fetchall()])
        
        # Lọc fields chỉ giữ lại những column có trong bảng
        filtered_fields = {k: v for k, v in fields.items() if k in existing_cols}
        
        if not filtered_fields:
            print(f"  Warning: Không có field nào hợp lệ cho record {obj.get('id', 'unknown')}")
            continue
            
        columns = ', '.join([escape_column_name(col) for col in filtered_fields.keys()])
        placeholders = ', '.join(['?'] * len(filtered_fields))
        values = list(filtered_fields.values())
        
        if 'id' in filtered_fields:
            set_clause = ', '.join([f'Target.{escape_column_name(col)} = Source.{escape_column_name(col)}' for col in filtered_fields.keys() if col != 'id'])
            def sql_value(val):
                if val is None:
                    return 'NULL'
                if isinstance(val, str):
                    return "N'" + val.replace("'", "''") + "'"
                return "N'" + str(val).replace("'", "''") + "'"
            select_cols = ', '.join([f"{sql_value(filtered_fields[col])} AS {escape_column_name(col)}" for col in filtered_fields.keys()])
            merge_sql = (
                f"MERGE INTO {escape_column_name(table_name)} AS Target "
                f"USING (SELECT {select_cols}) AS Source "
                f"ON Target.[id] = Source.[id] "
                f"WHEN MATCHED THEN UPDATE SET {set_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({', '.join([sql_value(filtered_fields[col]) for col in filtered_fields.keys()])});"
            )
            try:
                db_manager.cursor.execute(merge_sql)
            except Exception as e:
                print(f"  Error khi merge record {obj.get('id', 'unknown')}: {e}")
        else:
            sql = f'INSERT INTO {escape_column_name(table_name)} ({columns}) VALUES ({placeholders})'
            try:
                db_manager.cursor.execute(sql, values)
            except Exception as e:
                print(f"  Error khi insert record {obj.get('id', 'unknown')}: {e}")

def insert_nested_tables_batch(db_manager, table_name: str, records: List[Dict[str, Any]]):
    """Insert batch records vào các bảng nested"""
    for obj in records:
        _, nested = prepare_main_table_data(obj)
        record_id = obj.get('id')
        
        for nested_field, nested_list in nested.items():
            sub_table = f"{table_name}_{nested_field}"
            
            for item in nested_list:
                # Thêm fk_id vào item
                item['fk_id'] = record_id
                
                # Tạo bảng nested nếu chưa có
                ensure_table_exists(db_manager, sub_table, item, parent=True)
                ensure_columns_exist(db_manager, sub_table, item, parent=True)
                
                # Lọc columns có trong bảng
                db_manager.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{sub_table}'")
                existing_cols = set([row[0] for row in db_manager.cursor.fetchall()])
                filtered_item = {k: v for k, v in item.items() if k in existing_cols}
                
                if not filtered_item:
                    continue
                
                # Insert vào bảng nested
                columns = ', '.join([escape_column_name(col) for col in filtered_item.keys()])
                placeholders = ', '.join(['?'] * len(filtered_item))
                values = list(filtered_item.values())
                
                sql = f'INSERT INTO {escape_column_name(sub_table)} ({columns}) VALUES ({placeholders})'
                try:
                    db_manager.cursor.execute(sql, values)
                except Exception as e:
                    print(f"  Error khi insert nested record: {e}")

def insert_or_update_customer_optimized(data: Dict[str, Any], table_name: str = "crm_data_customer"):
    """
    Phiên bản tối ưu hóa cho việc insert/update customer data
    - Xử lý batch processing
    - Tách riêng main table và nested tables
    - Giảm thiểu nested operations
    - Xử lý lỗi column không tồn tại
    """
    db_manager = DatabaseManagerCRM()
    try:
        if db_manager.connect():
            if 'data' in data and isinstance(data['data'], list):
                total_records = len(data['data'])
                print(f"Bắt đầu xử lý {total_records} records với phiên bản tối ưu...")
                
                # Xử lý theo batch
                batch_size = 50  # Giảm batch size để tránh timeout
                for i in range(0, total_records, batch_size):
                    batch = data['data'][i:i + batch_size]
                    batch_start = i + 1
                    batch_end = min(i + batch_size, total_records)
                    
                    print(f"Đang xử lý batch {batch_start}-{batch_end}/{total_records}...")
                    start_time = time.time()
                    
                    # Xử lý bảng chính trước
                    print(f"  Đang xử lý bảng chính...")
                    insert_main_table_batch(db_manager, table_name, batch)
                    
                    # Commit bảng chính
                    db_manager.conn.commit()
                    
                    # Xử lý các bảng nested
                    print(f"  Đang xử lý các bảng nested...")
                    insert_nested_tables_batch(db_manager, table_name, batch)
                    
                    # Commit nested tables
                    db_manager.conn.commit()
                    
                    end_time = time.time()
                    print(f"  Hoàn thành batch {batch_start}-{batch_end} trong {end_time - start_time:.2f}s")
                
                print(f"Đã xử lý xong tất cả {total_records} records!")
                
            elif isinstance(data, dict):
                # Xử lý single record
                fields, nested = prepare_main_table_data(data)
                ensure_table_exists(db_manager, table_name, data, parent=False)
                ensure_columns_exist(db_manager, table_name, data, parent=False)
                
                columns = ', '.join([escape_column_name(col) for col in fields.keys()])
                placeholders = ', '.join(['?'] * len(fields))
                values = list(fields.values())
                
                sql = f'INSERT INTO {escape_column_name(table_name)} ({columns}) VALUES ({placeholders})'
                db_manager.cursor.execute(sql, values)
                db_manager.conn.commit()
                
    finally:
        db_manager.close() 