from database.manager import DatabaseManagerCRM
import json
from typing import Dict, Any
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
    # Sử dụng kiểu dữ liệu phù hợp cho SQL Server
    if isinstance(value, bool):
        return 'NVARCHAR(MAX)'
    if isinstance(value, int):
        return 'NVARCHAR(MAX)'
    if isinstance(value, float):
        return 'NVARCHAR(MAX)'
    if isinstance(value, str):
        return 'NVARCHAR(MAX)'
    if isinstance(value, list):
        return 'NVARCHAR(MAX)'
    if isinstance(value, dict):
        return 'NVARCHAR(MAX)'
    return 'NVARCHAR(MAX)'

def ensure_table_exists(db_manager, table, obj, parent=False):
    # Kiểm tra bảng đã tồn tại chưa (SQL Server)
    check_sql = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table}'"
    db_manager.cursor.execute(check_sql)
    if not db_manager.cursor.fetchone():
        columns = []
        for k, v in obj.items():
            # Chỉ tạo column cho các field có trong response thực tế
            if isinstance(v, list) and v and isinstance(v[0], dict):
                # Bỏ qua các nested list, sẽ tạo bảng riêng
                continue
            elif isinstance(v, dict):
                # Chuyển object thành JSON string
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
        # Chỉ thêm column cho các field có trong response thực tế
        if isinstance(v, list) and v and isinstance(v[0], dict):
            # Bỏ qua các nested list
            continue
        if k not in existing_cols:
            db_manager.cursor.execute(f'ALTER TABLE {escape_column_name(table)} ADD {escape_column_name(k)} {get_sqlite_type(v)}')
            existing_cols.add(k)
    if parent and 'fk_id' not in existing_cols:
        db_manager.cursor.execute(f'ALTER TABLE {escape_column_name(table)} ADD {escape_column_name("fk_id")} NVARCHAR(255)')
        existing_cols.add('fk_id')

def insert_or_update_customer(data: Dict[str, Any], table_name: str = "crm_data_customer", db_path: str = "database/manager.db"):
    """
    Insert/update dữ liệu vào bảng chính và các bảng phụ (nếu có nested list object),
    thêm trường fk_id vào bảng phụ. Nếu thiếu column/table thì tự động tạo.
    Tối ưu hóa cho xử lý số lượng lớn records.
    """
    def flatten_and_insert(db_manager, obj, table, parent_id=None):
        ensure_table_exists(db_manager, table, obj, parent=bool(parent_id))
        ensure_columns_exist(db_manager, table, obj, parent=bool(parent_id))
        fields = {}
        nested = {}
        for k, v in obj.items():
            # Chỉ xử lý các field có trong response thực tế
            if isinstance(v, list) and v and isinstance(v[0], dict):
                nested[k] = v
            elif isinstance(v, (dict, list)):
                fields[k] = json.dumps(v, ensure_ascii=False)
            else:
                fields[k] = v
        if parent_id:
            fields['fk_id'] = parent_id
        columns = ', '.join([escape_column_name(col) for col in fields.keys()])
        placeholders = ', '.join(['?'] * len(fields))
        values = list(fields.values())
        if 'id' in fields:
            set_clause = ', '.join([f'Target.{escape_column_name(col)} = Source.{escape_column_name(col)}' for col in fields.keys() if col != 'id'])
            def sql_value(val):
                if val is None:
                    return 'NULL'
                if isinstance(val, str):
                    return "N'" + val.replace("'", "''") + "'"
                return "N'" + str(val).replace("'", "''") + "'"
            select_cols = ', '.join([f"{sql_value(fields[col])} AS {escape_column_name(col)}" for col in fields.keys()])
            merge_sql = (
                f"MERGE INTO {escape_column_name(table)} AS Target "
                f"USING (SELECT {select_cols}) AS Source "
                f"ON Target.[id] = Source.[id] "
                f"WHEN MATCHED THEN UPDATE SET {set_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({', '.join([sql_value(fields[col]) for col in fields.keys()])});"
            )
            db_manager.cursor.execute(merge_sql)
        else:
            sql = f'INSERT INTO {escape_column_name(table)} ({columns}) VALUES ({placeholders})'
            db_manager.cursor.execute(sql, values)
        record_id = fields.get('id', parent_id)
        for nested_field, nested_list in nested.items():
            sub_table = f"{table}_{nested_field}"
            for item in nested_list:
                flatten_and_insert(db_manager, item, sub_table, parent_id=record_id)
    
    db_manager = DatabaseManagerCRM()
    try:
        if db_manager.connect():
            if 'data' in data and isinstance(data['data'], list):
                total_records = len(data['data'])
                print(f"Bắt đầu xử lý {total_records} records...")
                
                # Xử lý theo batch để tránh timeout
                batch_size = 100
                for i in range(0, total_records, batch_size):
                    batch = data['data'][i:i + batch_size]
                    batch_start = i + 1
                    batch_end = min(i + batch_size, total_records)
                    
                    print(f"Đang xử lý batch {batch_start}-{batch_end}/{total_records}...")
                    start_time = time.time()
                    
                    for j, obj in enumerate(batch):
                        if j % 50 == 0:  # Log progress mỗi 50 records
                            print(f"  Đang xử lý record {batch_start + j}/{total_records}")
                        flatten_and_insert(db_manager, obj, table_name)
                    
                    # Commit sau mỗi batch
                    db_manager.conn.commit()
                    end_time = time.time()
                    print(f"  Hoàn thành batch {batch_start}-{batch_end} trong {end_time - start_time:.2f}s")
                
                print(f"Đã xử lý xong tất cả {total_records} records!")
                
            elif isinstance(data, dict):
                flatten_and_insert(db_manager, data, table_name)
                db_manager.conn.commit()
    finally:
        db_manager.close()
