from database.manager import DatabaseManager
import uuid

def escape_column_name(column_name: str) -> str:
    """Escape tên cột nếu là từ khóa SQL"""
    reserved_keywords = {
        'order', 'group', 'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'outer',
        'having', 'by', 'desc', 'asc', 'top', 'distinct', 'union', 'all', 'insert', 'update',
        'delete', 'create', 'drop', 'alter', 'table', 'view', 'index', 'primary', 'foreign',
        'key', 'references', 'constraint', 'default', 'check', 'unique', 'null', 'not', 'and',
        'or', 'in', 'exists', 'between', 'like', 'is', 'as', 'on', 'using', 'natural', 'cross'
    }
    return f"[{column_name}]" if column_name.lower() in reserved_keywords else column_name

def safe_field_name(field):
    """Xử lý tên field đặc biệt"""
    return 'kill_flag' if field == 'kill' else field

def convert_value_for_sql(value):
    """Chuyển đổi giá trị để phù hợp với SQL Server"""
    if isinstance(value, bool):
        return 1 if value else 0
    elif isinstance(value, list):
        return None  # Bỏ qua list, sẽ xử lý riêng
    elif value is None:
        return None
    else:
        return str(value) if value is not None else None

def insert_or_update_customer(result, table_name="crm_data_customer"):
    """
    Insert hoặc update dữ liệu customer vào bảng chính và các bảng con
    """
    if not result or 'data' not in result or not isinstance(result['data'], list) or not result['data']:
        print("Không có dữ liệu customer để insert/update")
        return

    data_list = result['data']
    db = DatabaseManager()
    if not db.connect():
        print("Không thể kết nối database")
        return

    def check_column_exists(table, column):
        """Kiểm tra cột có tồn tại trong bảng không"""
        try:
            query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND COLUMN_NAME = '{column}'"
            db.cursor.execute(query)
            return db.cursor.fetchone() is not None
        except Exception as e:
            print(f"Lỗi kiểm tra cột {column} trong bảng {table}: {e}")
            return False

    def get_existing_columns(table_name):
        """Lấy danh sách cột hiện có trong bảng"""
        try:
            db.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            return set(row[0] for row in db.cursor.fetchall())
        except Exception as e:
            print(f"Lỗi lấy danh sách cột bảng {table_name}: {e}")
            return set()

    def add_column_if_missing(table_name, column_name):
        """Thêm cột mới nếu chưa tồn tại"""
        try:
            alter_sql = f"ALTER TABLE {table_name} ADD {escape_column_name(column_name)} NVARCHAR(MAX)"
            db.cursor.execute(alter_sql)
            db.conn.commit()
            print(f"[AUTO] Đã thêm cột '{column_name}' vào bảng '{table_name}'")
        except Exception as e:
            print(f"Lỗi khi thêm cột '{column_name}' vào bảng '{table_name}': {e}")

    def ensure_columns_exist(table_name, data_dict):
        """Đảm bảo tất cả cột cần thiết đã tồn tại"""
        existing_cols = get_existing_columns(table_name)
        for col in data_dict.keys():
            if col not in existing_cols:
                add_column_if_missing(table_name, col)

    def check_table_exists(table_name):
        """Kiểm tra bảng có tồn tại không"""
        try:
            query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            db.cursor.execute(query)
            return db.cursor.fetchone() is not None
        except Exception as e:
            print(f"Lỗi kiểm tra bảng {table_name}: {e}")
            return False

    def upsert_recursive(data, table_name, parent_id=None):
        """
        Insert hoặc update dữ liệu vào bảng và các bảng con
        """
        if not data:
            return None

        # Tách các trường đơn giản và các trường nested
        simple_fields = {}
        nested_fields = {}
        __id_value = None
        id_value = None
        
        for key, value in data.items():
            # Lưu __id để kiểm tra trùng lặp
            if key == '__id':
                __id_value = value
                continue
            # Lưu id từ response để sử dụng làm khóa chính
            elif key == 'id':
                id_value = value
                continue
            # BỎ QUA fk_id và _id, sẽ được xử lý riêng
            elif key in ['fk_id', '_id']:
                continue
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                nested_fields[key] = value
            elif isinstance(value, dict):
                nested_fields[key] = [value]
            else:
                simple_fields[key] = convert_value_for_sql(value)

        # Thêm fk_id nếu có parent_id (cho bảng con)
        if parent_id is not None:
            simple_fields['fk_id'] = parent_id

        # Đảm bảo các cột đã tồn tại trong bảng
        columns_to_check = simple_fields.copy()
        if __id_value is not None:
            columns_to_check['__id'] = __id_value
        if id_value is not None:
            columns_to_check['id'] = id_value
        
        ensure_columns_exist(table_name, columns_to_check)

        is_main_table = parent_id is None
        update_values = []
        set_clauses = []
        record_id = None

        if is_main_table:
            # Bảng chính: kiểm tra theo __id hoặc id
            check_id = __id_value if __id_value is not None else id_value
            
            if check_id is not None:
                # Kiểm tra xem record đã tồn tại chưa dựa trên __id hoặc id
                if __id_value is not None:
                    check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('__id')} = ?"
                    check_param = __id_value
                else:
                    check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('id')} = ?"
                    check_param = id_value
                
                db.cursor.execute(check_sql, (check_param,))
                exist_row = db.cursor.fetchone()
                
                if exist_row:
                    # Đã tồn tại, update
                    print(f"DEBUG: Record với {'__id' if __id_value else 'id'}={check_id} đã tồn tại, thực hiện UPDATE")
                    for k, v in simple_fields.items():
                        set_clauses.append(f"{escape_column_name(safe_field_name(k))} = ?")
                        update_values.append(v)
                    
                    # Kiểm tra xem có cột nào để update không
                    if set_clauses:
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('__id' if __id_value else 'id')} = ?"
                        db.cursor.execute(update_sql, update_values + [check_param])
                        db.conn.commit()
                    else:
                        print(f"DEBUG: Không có cột nào để update cho record với {'__id' if __id_value else 'id'}={check_id}")
                    
                    # Trả về id của record đã tồn tại
                    record_id = exist_row[0]
                else:
                    # Chưa có, insert mới
                    print(f"DEBUG: Record với {'__id' if __id_value else 'id'}={check_id} chưa tồn tại, thực hiện INSERT")
                    # Thêm __id hoặc id vào dữ liệu để insert
                    if __id_value is not None:
                        simple_fields['__id'] = __id_value
                    if id_value is not None:
                        simple_fields['id'] = id_value
                    
                    columns = [escape_column_name(safe_field_name(k)) for k in simple_fields.keys()]
                    placeholders = ['?' for _ in simple_fields]
                    values = list(simple_fields.values())
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    db.cursor.execute(insert_sql, values)
                    db.conn.commit()
                    # Trả về id của record vừa insert
                    record_id = id_value if id_value is not None else __id_value
            else:
                # Không có __id hoặc id, insert như cũ
                print(f"DEBUG: Không có __id hoặc id, thực hiện INSERT mới")
                if simple_fields:
                    columns = [escape_column_name(safe_field_name(k)) for k in simple_fields.keys()]
                    placeholders = ['?' for _ in simple_fields]
                    values = list(simple_fields.values())
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    db.cursor.execute(insert_sql, values)
                    db.conn.commit()
                    db.cursor.execute("SELECT SCOPE_IDENTITY()")
                    result = db.cursor.fetchone()
                    if result and result[0] is not None:
                        try:
                            record_id = int(result[0])
                        except (ValueError, TypeError):
                            record_id = str(result[0])
                    else:
                        record_id = None
        else:
            # Bảng con: kiểm tra theo fk_id và id từ response
            check_id = id_value if id_value is not None else None
            
            if check_id is not None:
                # Kiểm tra theo id từ response
                check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('id')} = ?"
                db.cursor.execute(check_sql, (check_id,))
                exist_row = db.cursor.fetchone()
                if exist_row:
                    # Đã tồn tại, update
                    print(f"DEBUG: Bảng con record với id={check_id} đã tồn tại, thực hiện UPDATE")
                    for k, v in simple_fields.items():
                        if k != 'id':
                            set_clauses.append(f"{escape_column_name(safe_field_name(k))} = ?")
                            update_values.append(v)
                    
                    # Kiểm tra xem có cột nào để update không
                    if set_clauses:
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('id')} = ?"
                        db.cursor.execute(update_sql, update_values + [check_id])
                        db.conn.commit()
                    else:
                        print(f"DEBUG: Không có cột nào để update cho record với id={check_id}")
                    
                    record_id = exist_row[0]
                else:
                    # Chưa có, insert mới với id từ response
                    print(f"DEBUG: Bảng con record với id={check_id} chưa tồn tại, thực hiện INSERT")
                    simple_fields['id'] = id_value
                    columns = [escape_column_name(safe_field_name(k)) for k in simple_fields.keys()]
                    placeholders = ['?' for _ in simple_fields]
                    values = list(simple_fields.values())
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    db.cursor.execute(insert_sql, values)
                    db.conn.commit()
                    db.cursor.execute("SELECT SCOPE_IDENTITY()")
                    result = db.cursor.fetchone()
                    if result and result[0] is not None:
                        try:
                            record_id = int(result[0])
                        except (ValueError, TypeError):
                            record_id = str(result[0])
                    else:
                        record_id = None
            elif 'fk_id' in simple_fields:
                # Fallback: kiểm tra theo fk_id nếu không có id từ response
                check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('fk_id')} = ?"
                db.cursor.execute(check_sql, (simple_fields['fk_id'],))
                exist_row = db.cursor.fetchone()
                if exist_row:
                    # Đã tồn tại, update
                    print(f"DEBUG: Bảng con record với fk_id={simple_fields['fk_id']} đã tồn tại, thực hiện UPDATE")
                    for k, v in simple_fields.items():
                        if k != 'fk_id':
                            set_clauses.append(f"{escape_column_name(safe_field_name(k))} = ?")
                            update_values.append(v)
                    
                    # Kiểm tra xem có cột nào để update không
                    if set_clauses:
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('fk_id')} = ?"
                        db.cursor.execute(update_sql, update_values + [simple_fields['fk_id']])
                        db.conn.commit()
                    else:
                        print(f"DEBUG: Không có cột nào để update cho record với fk_id={simple_fields['fk_id']}")
                    
                    record_id = exist_row[0]
                else:
                    # Chưa có, insert
                    print(f"DEBUG: Bảng con record với fk_id={simple_fields['fk_id']} chưa tồn tại, thực hiện INSERT")
                    columns = [escape_column_name(safe_field_name(k)) for k in simple_fields.keys()]
                    placeholders = ['?' for _ in simple_fields]
                    values = list(simple_fields.values())
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    db.cursor.execute(insert_sql, values)
                    db.conn.commit()
                    db.cursor.execute("SELECT SCOPE_IDENTITY()")
                    result = db.cursor.fetchone()
                    if result and result[0] is not None:
                        try:
                            record_id = int(result[0])
                        except (ValueError, TypeError):
                            record_id = str(result[0])
                    else:
                        record_id = None
            else:
                # Không có fk_id, insert như cũ
                columns = [escape_column_name(safe_field_name(k)) for k in simple_fields.keys()]
                placeholders = ['?' for _ in simple_fields]
                values = list(simple_fields.values())
                insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                db.cursor.execute(insert_sql, values)
                db.conn.commit()
                db.cursor.execute("SELECT SCOPE_IDENTITY()")
                result = db.cursor.fetchone()
                if result and result[0] is not None:
                    try:
                        record_id = int(result[0])
                    except (ValueError, TypeError):
                        record_id = str(result[0])
                else:
                    record_id = None

        # Xử lý các trường nested (bảng con)
        for field_name, nested_data in nested_fields.items():
            if not nested_data:
                continue
            child_table_name = f"{table_name}_{field_name}"
            if not check_table_exists(child_table_name):
                print(f"DEBUG: Bảng con {child_table_name} không tồn tại, bỏ qua")
                continue
            print(f"DEBUG: Xử lý list {child_table_name}, số lượng: {len(nested_data)}")
            for i, item in enumerate(nested_data):
                print(f"DEBUG: Xử lý item {i} trong {child_table_name}")
                # Truyền id của bảng cha (record_id) làm fk_id cho bảng con
                child_id = upsert_recursive(item, child_table_name, record_id)
                if child_id is None:
                    print(f"DEBUG: Không thể insert/update vào {child_table_name} - có thể do lỗi SQL hoặc dữ liệu không hợp lệ")
                else:
                    print(f"DEBUG: Đã xử lý thành công item {i} trong {child_table_name} với ID: {child_id}")
        
        return record_id

    def process_nested_data_recursive(data, parent_table, parent_id):
        """
        Xử lý đệ quy các nested data nhiều level
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (dict, list)) and value:
                    child_table_name = f"{parent_table}_{key}"
                    if check_table_exists(child_table_name):
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict):
                                    child_id = upsert_recursive(item, child_table_name, parent_id)
                                    if child_id:
                                        # Đệ quy xử lý nested data bên trong
                                        process_nested_data_recursive(item, child_table_name, child_id)
                        elif isinstance(value, dict):
                            child_id = upsert_recursive(value, child_table_name, parent_id)
                            if child_id:
                                # Đệ quy xử lý nested data bên trong
                                process_nested_data_recursive(value, child_table_name, child_id)

    try:
        # Xử lý từng customer
        for i, customer_data in enumerate(data_list, 1):
            print(f"\n=== Xử lý customer {i} ===")
            
            # Insert/update customer chính
            customer_id = upsert_recursive(customer_data, table_name)
            
            if customer_id:
                print(f"DEBUG: Customer {i} processed successfully with ID: {customer_id}")
                # Xử lý nested data nhiều level
                process_nested_data_recursive(customer_data, table_name, customer_id)
            else:
                print(f"DEBUG: Failed to process customer {i}")

        db.conn.commit()
        print(f"\nĐã xử lý xong {len(data_list)} customer")

    except Exception as e:
        print(f"Lỗi trong quá trình insert/update: {e}")
        db.conn.rollback()
    finally:
        db.close()
