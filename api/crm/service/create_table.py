import json
from service.createTable import create_table
from database.manager import DatabaseManager

def safe_field_name(field):
    return 'kill_flag' if field == 'kill' else field

def analyze_object_to_table_structure(object_data: dict, parent_table_name: str) -> list:
    """
    Đệ quy phân tích object mẫu thành danh sách (table_name, table_structure) cho mọi bảng cần tạo
    """
    fields = []
    tables = []
    for key, value in object_data.items():
        field_type = None
        field_name = safe_field_name(key)
        if isinstance(value, list):
            # Nếu là list các dict, gộp tất cả các key của các dict trong list lại
            if len(value) > 0 and all(isinstance(v, dict) for v in value):
                merged = {}
                for v in value:
                    merged.update(v)
                child_table_name = f"{parent_table_name}_{field_name}"
                child_structs = analyze_object_to_table_structure(merged, child_table_name)
                tables.extend(child_structs)
                field_type = {'type': 'array', 'items': 'object'}
            else:
                field_type = 'array'
        elif isinstance(value, dict):
            child_table_name = f"{parent_table_name}_{field_name}"
            child_structs = analyze_object_to_table_structure(value, child_table_name)
            tables.extend(child_structs)
            field_type = {'type': 'object'}
        elif isinstance(value, int):
            field_type = 'int'
        elif isinstance(value, float):
            field_type = 'number'
        elif isinstance(value, bool):
            field_type = 'boolean'
        else:
            field_type = 'string'
        fields.append({'field': field_name, 'type': field_type})
    if not any(f['field'].lower() == 'id' for f in fields):
        fields.insert(0, {'field': 'id', 'type': 'int'})
    tables.insert(0, (parent_table_name, fields))
    return tables

def print_table_columns(db_manager, table_name):
    db_manager.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
    print(f"[DB] {table_name} columns: {[row[0] for row in db_manager.cursor.fetchall()]}")

def create_table_from_object(object_data: dict, table_name: str):
    tables = analyze_object_to_table_structure(object_data, table_name)
    db_manager = DatabaseManager()
    db_manager.connect()
    for tbl_name, tbl_struct in tables:
        print(f"[CREATE] {tbl_name} columns: {[f['field'] for f in tbl_struct]}")
        create_table(tbl_struct, tbl_name)
        print_table_columns(db_manager, tbl_name)
    db_manager.close()
