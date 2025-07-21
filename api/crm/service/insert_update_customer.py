from database.manager import DatabaseManagerCRM
import uuid

def escape_column_name(column_name: str) -> str:
    reserved_keywords = {
        'order', 'group', 'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'outer',
        'having', 'by', 'desc', 'asc', 'top', 'distinct', 'union', 'all', 'insert', 'update',
        'delete', 'create', 'drop', 'alter', 'table', 'view', 'index', 'primary', 'foreign',
        'key', 'references', 'constraint', 'default', 'check', 'unique', 'null', 'not', 'and',
        'or', 'in', 'exists', 'between', 'like', 'is', 'as', 'on', 'using', 'natural', 'cross'
    }
    return f"[{column_name}]" if column_name.lower() in reserved_keywords else column_name

def safe_field_name(field):
    return 'kill_flag' if field == 'kill' else field

def convert_value_for_sql(value):
    if isinstance(value, bool):
        return str(int(value))
    elif isinstance(value, list):
        return str(value)
    elif value is None:
        return None
    else:
        return str(value) if value is not None else None

def remove_data_key_recursive(obj):
    if isinstance(obj, dict):
        obj.pop('data', None)
        for v in obj.values():
            remove_data_key_recursive(v)
    elif isinstance(obj, list):
        for item in obj:
            remove_data_key_recursive(item)

def ensure_columns_exist_for_bulk(table_name, list_of_dicts):
    """Đảm bảo tất cả các cột cần thiết đã tồn tại trước khi MERGE/bulk insert/update"""
    all_keys = set()
    for row in list_of_dicts:
        all_keys.update(row.keys())
    db = DatabaseManagerCRM()
    if not db.connect():
        return
    try:
        db.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        existing_cols = set(row[0] for row in db.cursor.fetchall())
        for col in all_keys:
            if col not in existing_cols:
                alter_sql = f"ALTER TABLE {table_name} ADD {escape_column_name(col)} NVARCHAR(MAX)"
                db.cursor.execute(alter_sql)
                db.conn.commit()
    except Exception as e:
        print(f"[ensure_columns_exist_for_bulk] Lỗi: {e}")
    finally:
        db.close()

_ = ensure_columns_exist_for_bulk  # suppress warning if unused

def insert_or_update_customer(result, table_name="crm_data_customer"):
    if not result or 'data' not in result or not isinstance(result['data'], list) or not result['data']:
        print("Không có dữ liệu customer để insert/update")
        return

    data_list = result['data']
    db = DatabaseManagerCRM()
    if not db.connect():
        print("Không thể kết nối database")
        return

    def get_existing_columns(table_name):
        try:
            db.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            return set(row[0] for row in db.cursor.fetchall())
        except Exception as e:
            return set()

    def add_column_if_missing(table_name, column_name):
        try:
            alter_sql = f"ALTER TABLE {table_name} ADD {escape_column_name(column_name)} NVARCHAR(MAX)"
            db.cursor.execute(alter_sql)
            db.conn.commit()
        except Exception as e:
            pass

    def ensure_columns_exist(table_name, data_dict):
        existing_cols = get_existing_columns(table_name)
        for col in data_dict.keys():
            if col not in existing_cols:
                add_column_if_missing(table_name, col)
        for col in data_dict.keys():
            if col not in existing_cols:
                print(f"[LOG][AUTO ALTER] Thêm cột mới: {col} vào bảng {table_name}")
                add_column_if_missing(table_name, col)

    def upsert_recursive(data, table_name, parent_id=None):
        if not data:
            return None
        simple_fields = {}
        nested_fields = {}
        __id_value = None
        id_value = None
        for key, value in data.items():
            if key == 'data':
                continue
            if key == '__id':
                __id_value = value
                continue
            elif key == 'id':
                id_value = value
                continue
            elif key in ['fk_id', '_id']:
                continue
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                nested_fields[key] = value
                simple_fields[f"{key}_ids"] = None
            elif isinstance(value, dict):
                nested_fields[key] = [value]
                simple_fields[f"{key}_id"] = None
            else:
                # Chuyển đổi dữ liệu phức tạp sang string
                if isinstance(value, (list, dict, bool)):
                    simple_fields[key] = str(value)
                else:
                    simple_fields[key] = convert_value_for_sql(value)
        simple_fields.pop('data', None)
        nested_fields.pop('data', None)
        if parent_id is not None:
            simple_fields['fk_id'] = parent_id
        # Đảm bảo bảng con có đủ cột trước khi insert/update
        ensure_columns_exist_for_bulk(table_name, [simple_fields])
        existing_cols = get_existing_columns(table_name)
        for col in existing_cols:
            if (col.endswith('_ids') or col.endswith('_id')) and col not in simple_fields:
                simple_fields[col] = ""
        existing_cols = get_existing_columns(table_name)
        filtered_simple_fields = {k: v for k, v in simple_fields.items() if k in existing_cols}
        filtered_simple_fields.pop('data', None)
        if 'data' in filtered_simple_fields:
            del filtered_simple_fields['data']
        ensure_columns_exist(table_name, {k: v for k, v in filtered_simple_fields.items() if k.endswith('_ids') or k.endswith('_id')})
        for k in filtered_simple_fields:
            if (k.endswith('_ids') or k.endswith('_id')) and k not in existing_cols:
                print(f"[LOG][WARN] Đã thêm cột reference vật lý bị thiếu: {k} vào bảng {table_name}")
        record_id = None
        is_main_table = parent_id is None
        update_values = []
        set_clauses = []
        if is_main_table:
            if id_value is not None:
                check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('id')} = ?"
                db.cursor.execute(check_sql, (id_value,))
                exist_row = db.cursor.fetchone()
            else:
                exist_row = None
            if exist_row:
                for k, v in filtered_simple_fields.items():
                    set_clauses.append(f"{escape_column_name(safe_field_name(k))} = ?")
                    update_values.append(v)
                if set_clauses:
                    update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('id')} = ?"
                    db.cursor.execute(update_sql, update_values + [id_value])
                    db.conn.commit()
                record_id = exist_row[0]
            else:
                if id_value is not None:
                    filtered_simple_fields['id'] = id_value
                if __id_value is not None:
                    filtered_simple_fields['__id'] = __id_value
                columns = [escape_column_name(safe_field_name(k)) for k in filtered_simple_fields.keys()]
                placeholders = ['?' for _ in filtered_simple_fields]
                values = list(filtered_simple_fields.values())
                insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                db.cursor.execute(insert_sql, values)
                db.conn.commit()
                record_id = id_value if id_value is not None else __id_value
        else:
            check_id = id_value if id_value is not None else None
            check_fk_id = simple_fields.get('fk_id')
            check_value = simple_fields.get('value') if 'value' in simple_fields else None
            exist_row = None
            existing_cols = get_existing_columns(table_name)
            has_value_col = 'value' in existing_cols
            if check_fk_id is not None and has_value_col and check_value is not None:
                check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('fk_id')} = ? AND {escape_column_name('value')} = ?"
                db.cursor.execute(check_sql, (check_fk_id, check_value))
                exist_row = db.cursor.fetchone()
            elif check_fk_id is not None:
                check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('fk_id')} = ?"
                db.cursor.execute(check_sql, (check_fk_id,))
                exist_row = db.cursor.fetchone()
            elif has_value_col and check_value is not None:
                check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('value')} = ?"
                db.cursor.execute(check_sql, (check_value,))
                exist_row = db.cursor.fetchone()
            elif check_id is not None:
                check_sql = f"SELECT id FROM {table_name} WHERE {escape_column_name('id')} = ?"
                db.cursor.execute(check_sql, (check_id,))
                exist_row = db.cursor.fetchone()
            else:
                exist_row = None
            if exist_row:
                for k, v in filtered_simple_fields.items():
                    if k != 'id':
                        set_clauses.append(f"{escape_column_name(safe_field_name(k))} = ?")
                        update_values.append(v)
                if set_clauses:
                    if check_fk_id is not None and has_value_col and check_value is not None:
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('fk_id')} = ? AND {escape_column_name('value')} = ?"
                        db.cursor.execute(update_sql, update_values + [check_fk_id, check_value])
                    elif check_fk_id is not None:
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('fk_id')} = ?"
                        db.cursor.execute(update_sql, update_values + [check_fk_id])
                    elif has_value_col and check_value is not None:
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('value')} = ?"
                        db.cursor.execute(update_sql, update_values + [check_value])
                    elif check_id is not None:
                        update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {escape_column_name('id')} = ?"
                        db.cursor.execute(update_sql, update_values + [check_id])
                    db.conn.commit()
                record_id = exist_row[0]
            else:
                if 'id' not in filtered_simple_fields or not filtered_simple_fields['id']:
                    filtered_simple_fields['id'] = uuid.uuid4().hex
                columns = [escape_column_name(safe_field_name(k)) for k in filtered_simple_fields.keys()]
                placeholders = ['?' for _ in filtered_simple_fields]
                values = list(filtered_simple_fields.values())
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
                    record_id = filtered_simple_fields['id']
        def log_nested(field_name, msg, record_id=None):
            pass
        if record_id is not None:
            nested_ids_list = []
            for field_name, nested_data in nested_fields.items():
                if not nested_data:
                    continue
                child_table_name = f"{table_name}_{field_name}"
                if not get_existing_columns(child_table_name):
                    continue
                # Tự động thêm cột mới vào bảng nested nếu có
                for item in nested_data:
                    if isinstance(item, dict):
                        ensure_columns_exist(child_table_name, item)
                field_ids = []
                for i, item in enumerate(nested_data):
                    child_id = upsert_recursive(item, child_table_name, record_id)
                    if child_id is not None:
                        field_ids.append(str(child_id))
                if isinstance(nested_data, list):
                    ref_col = f"{field_name}_ids"
                else:
                    ref_col = f"{field_name}_id"
                if field_ids:
                    nested_ids_list.append((ref_col, ','.join(field_ids)))
                    simple_fields[ref_col] = ','.join(field_ids)
            ensure_columns_exist(table_name, {k: v for k, v in simple_fields.items() if k.endswith('_ids') or k.endswith('_id')})
            existing_cols = get_existing_columns(table_name)
            filtered_simple_fields = {k: v for k, v in simple_fields.items() if k in existing_cols}
            if len(filtered_simple_fields) > 0:
                try:
                    update_clauses = []
                    update_values = []
                    for k, v in filtered_simple_fields.items():
                        if k != 'id':
                            update_clauses.append(f"{escape_column_name(safe_field_name(k))} = ?")
                            update_values.append(v)
                    if update_clauses:
                        update_sql = f"UPDATE {table_name} SET {', '.join(update_clauses)} WHERE {escape_column_name('id')} = ?"
                        db.cursor.execute(update_sql, update_values + [record_id])
                        db.conn.commit()
                except Exception as e:
                    pass
        return record_id
    try:
        processed_count = 0
        total_count = len(data_list)
        for i, customer_data in enumerate(data_list, 1):
            remove_data_key_recursive(customer_data)
            customer_id = upsert_recursive(customer_data, table_name)
            if customer_id:
                processed_count += 1
            if i % 100 == 0 or i == total_count:
                print(f"{i}/{total_count}@insert_update.py")
        db.conn.commit()
        print(f"Đã xử lý thành công {processed_count}/{total_count} customer")
    except Exception as e:
        print(f"Lỗi trong quá trình insert/update: {e}")
        db.conn.rollback()
    finally:
        db.close()

def merge_bulk_to_child_table(table_name, list_of_dicts, db_manager=None):
    """
    MERGE dữ liệu vào bảng con, đảm bảo tự động thêm cột mới và chỉ dùng các cột thực tế của bảng.
    """
    ensure_columns_exist_for_bulk(table_name, list_of_dicts)
    # Chuẩn hóa dữ liệu: chuyển list, dict, bool thành chuỗi
    for row in list_of_dicts:
        for k, v in row.items():
            if isinstance(v, (list, dict, bool)):
                row[k] = str(v)
    db = db_manager or DatabaseManagerCRM()
    if not db.connect():
        print(f"Không thể kết nối database để MERGE vào {table_name}")
        return
    try:
        # Lấy danh sách cột thực tế của bảng con
        db.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        real_columns = set(row[0] for row in db.cursor.fetchall())
        if not real_columns:
            print(f"[LỖI] Không lấy được danh sách cột của bảng {table_name}")
            return
        # Tạo bảng tạm
        temp_table = f'temp_{table_name}'
        create_temp_table_like_main(db, table_name, temp_table)
        # Lấy danh sách cột thực tế của bảng chính
        db.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        main_columns = set(row[0] for row in db.cursor.fetchall())
        # Tự động thêm cột thiếu vào bảng chính
        all_data_keys = set()
        for row in list_of_dicts:
            all_data_keys.update(row.keys())
        for col in all_data_keys:
            if col not in main_columns:
                try:
                    alter_sql = f"ALTER TABLE {table_name} ADD {escape_column_name(col)} NVARCHAR(MAX)"
                    db.cursor.execute(alter_sql)
                    db.conn.commit()
                    print(f"[AUTO ALTER] Đã thêm cột {col} vào bảng {table_name}")
                    main_columns.add(col)
                except Exception as e:
                    print(f"[LỖI] Không thể thêm cột {col} vào bảng {table_name}: {e}")
        # Lấy danh sách cột thực tế của bảng tạm
        db.cursor.execute(f"SELECT COLUMN_NAME FROM tempdb.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{temp_table}'")
        temp_columns = set(row[0] for row in db.cursor.fetchall())
        if not temp_columns:
            print(f"[LỖI] Không lấy được danh sách cột của bảng tạm {temp_table}")
            return
        temp_columns = {c for c in temp_columns if not c.startswith('sys')}
        temp_columns_no_id = [c for c in temp_columns if c != 'id']
        if not temp_columns_no_id:
            print(f"[LỖI] Bảng tạm {temp_table} không có cột nào ngoài 'id', bỏ qua MERGE!")
            return
        # Chỉ lấy các cột có ở cả bảng chính và bảng tạm
        valid_columns = [c for c in temp_columns if c in main_columns]
        if not valid_columns:
            print(f"[LỖI] Không có cột hợp lệ để MERGE vào {table_name}")
            return
        filtered_data = []
        for row in list_of_dicts:
            filtered_row = {k: v for k, v in row.items() if k in valid_columns}
            filtered_data.append(filtered_row)
        columns = [c for c in valid_columns if any(c in row for row in filtered_data)]
        if not columns:
            print(f"[LỖI] Không có dữ liệu hợp lệ để MERGE vào {table_name}")
            return
        col_str = ', '.join(f'[{c}]' for c in columns)
        source_col_str = ', '.join(f'source.[{c}]' for c in columns)
        update_set = ', '.join(f'target.[{c}] = source.[{c}]' for c in columns if c != 'id')
        # Insert vào bảng tạm chỉ với các cột hợp lệ
        bulk_insert_temp_table(db, temp_table, [{k: v for k, v in row.items() if k in columns} for row in filtered_data])
        merge_query = f'''
            MERGE {table_name} AS target
            USING #{temp_table} AS source
            ON (target.[id] = source.[id])
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({col_str}) VALUES ({source_col_str});
        '''
        db.cursor.execute(merge_query)
        db.conn.commit()
        db.cursor.execute(f"DROP TABLE #{temp_table}")
        db.conn.commit()
        print(f"Đã MERGE thành công vào bảng {table_name}")
    except Exception as e:
        print(f"Lỗi khi MERGE vào bảng {table_name}: {e}")
    finally:
        db.close()

def create_temp_table_like_main(db, main_table, temp_table):
    db.cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{main_table}'
    """)
    columns = db.cursor.fetchall()
    col_defs = []
    for col in columns:
        name, dtype, maxlen = col
        if dtype.lower() == 'nvarchar' and maxlen:
            col_defs.append(f"[{name}] NVARCHAR({maxlen if maxlen > 0 else 'MAX'})")
        else:
            col_defs.append(f"[{name}] {dtype.upper()}")
    col_defs_str = ', '.join(col_defs)
    db.cursor.execute(f"IF OBJECT_ID('tempdb..#{temp_table}') IS NOT NULL DROP TABLE #{temp_table}")
    db.cursor.execute(f"CREATE TABLE #{temp_table} ({col_defs_str})")
    db.conn.commit()

def bulk_insert_temp_table(db, temp_table, list_of_dicts):
    for row in list_of_dicts:
        cols = ', '.join(f"[{k}]" for k in row.keys())
        vals = ', '.join('?' for _ in row)
        db.cursor.execute(f"INSERT INTO #{temp_table} ({cols}) VALUES ({vals})", tuple(row.values()))
    db.conn.commit()

def merge_temp_to_main(db, main_table, temp_table, key='id'):
    db.cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{main_table}'
    """)
    cols = [row[0] for row in db.cursor.fetchall()]
    set_clause = ', '.join(f"target.[{c}] = source.[{c}]" for c in cols if c != key)
    insert_cols = ', '.join(f"[{c}]" for c in cols)
    insert_vals = ', '.join(f"source.[{c}]" for c in cols)
    merge_sql = f"""
        MERGE {main_table} AS target
        USING #{temp_table} AS source
        ON (target.[{key}] = source.[{key}])
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals});
    """
    db.cursor.execute(merge_sql)
    db.conn.commit()
    db.cursor.execute(f"DROP TABLE #{temp_table}")
    db.conn.commit()

# HƯỚNG DẪN SỬ DỤNG:
# db = DatabaseManagerCRM()
# if db.connect():
#     ensure_columns_exist_for_bulk('crm_data_customer_phones', phones_data)
#     create_temp_table_like_main(db, 'crm_data_customer_phones', 'temp_crm_data_customer_phones')
#     bulk_insert_temp_table(db, 'temp_crm_data_customer_phones', phones_data)
#     merge_temp_to_main(db, 'crm_data_customer_phones', 'temp_crm_data_customer_phones')
#     db.close()
