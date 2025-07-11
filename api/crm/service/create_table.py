import json
from database.manager import DatabaseManager
from typing import Dict, List, Any, Set

# Escape tên cột nếu là từ khóa SQL
def escape_column_name(column_name: str) -> str:
    reserved_keywords = {
        'order', 'group', 'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'outer',
        'having', 'by', 'desc', 'asc', 'top', 'distinct', 'union', 'all', 'insert', 'update',
        'delete', 'create', 'drop', 'alter', 'table', 'view', 'index', 'primary', 'foreign',
        'key', 'references', 'constraint', 'default', 'check', 'unique', 'null', 'not', 'and',
        'or', 'in', 'exists', 'between', 'like', 'is', 'as', 'on', 'using', 'natural', 'cross'
    }
    return f"[{column_name}]" if column_name.lower() in reserved_keywords else column_name

# Mapping kiểu dữ liệu Python sang SQL Server
def get_sql_type(value: Any, field_name: str = None) -> str:
    if field_name is not None and field_name.lower() in ["encrypt", "encrypt_aes"]:
        return 'NVARCHAR(MAX)'
    if isinstance(value, bool):
        return 'NVARCHAR(10)'
    if isinstance(value, int):
        return 'NVARCHAR(50)'
    if isinstance(value, float):
        return 'NVARCHAR(50)'
    if isinstance(value, str):
        if len(value) > 255:
            return 'NVARCHAR(MAX)'
        return 'NVARCHAR(255)'
    if isinstance(value, list):
        return 'NVARCHAR(MAX)'
    if isinstance(value, dict):
        return 'NVARCHAR(MAX)'
    return 'NVARCHAR(255)'

# Kiểm tra cột đã tồn tại chưa
def check_column_exists(db_manager: DatabaseManager, table_name: str, column_name: str) -> bool:
    try:
        if db_manager.connect():
            query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            AND COLUMN_NAME = '{column_name}'
            """
            db_manager.cursor.execute(query)
            result = db_manager.cursor.fetchone()
            return result[0] > 0
    except Exception as e:
        pass
    return False

# Tạo bảng con cho object/array
def create_child_table(db_manager: DatabaseManager, parent_table: str, field_name: str, sample_value: Any):
    child_table_name = f"{parent_table}_{field_name}"
    columns = {
        'id': 'NVARCHAR(255) PRIMARY KEY',
        'fk_id': 'NVARCHAR(255)'
    }
    
    # Xử lý sample_value để lấy cấu trúc cột
    nested_fields_in_child = []  # Lưu các nested fields trong bảng con
    
    if isinstance(sample_value, dict):
        for k, v in sample_value.items():
            if isinstance(v, (dict, list)):
                nested_fields_in_child.append((k, v))
                # Thêm cột reference cho nested field trong bảng con
                if isinstance(v, list):
                    columns[escape_column_name(f"{k}_ids")] = 'NVARCHAR(MAX)'
                else:
                    columns[escape_column_name(f"{k}_id")] = 'NVARCHAR(255)'
            else:
                columns[escape_column_name(k)] = get_sql_type(v, k)
    elif isinstance(sample_value, list) and sample_value:
        first = sample_value[0]
        if isinstance(first, dict):
            for k, v in first.items():
                if isinstance(v, (dict, list)):
                    nested_fields_in_child.append((k, v))
                    # Thêm cột reference cho nested field trong bảng con
                    if isinstance(v, list):
                        columns[escape_column_name(f"{k}_ids")] = 'NVARCHAR(MAX)'
                    else:
                        columns[escape_column_name(f"{k}_id")] = 'NVARCHAR(255)'
                else:
                    columns[escape_column_name(k)] = get_sql_type(v, k)
        else:
            columns['value'] = get_sql_type(first, field_name)
    else:
        columns['value'] = get_sql_type(sample_value, field_name)
    
    # Tạo bảng con
    try:
        if db_manager.connect():
            check_table_query = f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{child_table_name}'
            """
            db_manager.cursor.execute(check_table_query)
            table_exists = db_manager.cursor.fetchone()[0] > 0
            if not table_exists:
                columns_str = ', '.join([f"{col} {col_type}" for col, col_type in columns.items()])
                create_table_sql = f"CREATE TABLE {child_table_name} ({columns_str})"
                db_manager.cursor.execute(create_table_sql)
                db_manager.conn.commit()
            else:
                for column_name, column_type in columns.items():
                    if not check_column_exists(db_manager, child_table_name, column_name):
                        alter_query = f"ALTER TABLE {child_table_name} ADD {column_name} {column_type}"
                        db_manager.cursor.execute(alter_query)
                        db_manager.conn.commit()
            
            # Tạo bảng con cho nested fields trong bảng con này
            for nested_field_name, nested_field_value in nested_fields_in_child:
                create_child_table(db_manager, child_table_name, nested_field_name, nested_field_value)
    except Exception as e:
        pass

# Hàm đệ quy để tạo bảng con cho nested data nhiều level
def create_nested_child_tables(db_manager: DatabaseManager, parent_table: str, data: Any):
    """
    Tạo bảng con cho tất cả các nested data
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)) and value:
                # Tạo bảng con cho field này
                create_child_table(db_manager, parent_table, key, value)
                # Đệ quy tạo bảng con cho nested data bên trong
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            create_nested_child_tables(db_manager, f"{parent_table}_{key}", item)
                elif isinstance(value, dict):
                    create_nested_child_tables(db_manager, f"{parent_table}_{key}", value)
    elif isinstance(data, list) and data:
        # Nếu data là list, xử lý item đầu tiên
        first_item = data[0]
        if isinstance(first_item, dict):
            create_nested_child_tables(db_manager, parent_table, first_item)

# Hàm chính tạo bảng động từ object
def create_table_from_object(data: Dict[str, Any], table_name: str):
    if not data or 'data' not in data or not data['data']:
        print(f"Không có dữ liệu để tạo bảng {table_name}")
        return
    
    sample_data = data['data'][0] if isinstance(data['data'], list) else data['data']
    db_manager = DatabaseManager()
    columns = {}
    nested_fields = []
    
    for key, value in sample_data.items():
        if isinstance(value, (dict, list)):
            nested_fields.append((key, value))
            # Thêm cột reference cho nested field
            if isinstance(value, list):
                columns[escape_column_name(f"{key}_ids")] = 'NVARCHAR(MAX)'  # Lưu danh sách ID
            else:
                columns[escape_column_name(f"{key}_id")] = 'NVARCHAR(255)'   # Lưu ID đơn
        else:
            columns[escape_column_name(key)] = get_sql_type(value, key)
    
    # Luôn có cột id
    if 'id' not in columns:
        columns['id'] = 'NVARCHAR(255) PRIMARY KEY'
    
    # Tạo bảng chính
    try:
        if db_manager.connect():
            check_table_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            db_manager.cursor.execute(check_table_query)
            table_exists = db_manager.cursor.fetchone()[0] > 0
            if not table_exists:
                columns_str = ', '.join([f"{col} {col_type}" for col, col_type in columns.items()])
                create_table_sql = f"CREATE TABLE {table_name} ({columns_str})"
                db_manager.cursor.execute(create_table_sql)
                db_manager.conn.commit()
                print(f"Đã tạo bảng chính: {table_name}")
            else:
                for column_name, column_type in columns.items():
                    if not check_column_exists(db_manager, table_name, column_name):
                        alter_query = f"ALTER TABLE {table_name} ADD {column_name} {column_type}"
                        db_manager.cursor.execute(alter_query)
                        db_manager.conn.commit()
    except Exception as e:
        print(f"Lỗi khi tạo bảng {table_name}: {e}")
    
    # Tạo bảng con cho nested fields và các nested data bên trong
    child_tables_count = 0
    for field_name, field_value in nested_fields:
        create_child_table(db_manager, table_name, field_name, field_value)
        # Tạo bảng con cho nested data bên trong
        create_nested_child_tables(db_manager, f"{table_name}_{field_name}", field_value)
        child_tables_count += 1
    
    db_manager.close()
    print(f"Đã tạo thành công bảng {table_name} và {child_tables_count} bảng con")
