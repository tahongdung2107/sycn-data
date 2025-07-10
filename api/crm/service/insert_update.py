from database.manager import DatabaseManager
import uuid

def escape_column_name(column_name: str) -> str:
    return f'[{column_name}]'

def safe_field_name(field):
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
        return value

def insert_or_update_customer(result, table_name="crm_data_customer_data"):
    if not result or 'data' not in result or not isinstance(result['data'], list) or not result['data']:
        print("Không có dữ liệu customer để insert/update")
        return

    data_list = result['data']
    db = DatabaseManager()
    if not db.connect():
        print("Không thể kết nối database")
        return

    def check_column_exists(table, column):
        try:
            query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND COLUMN_NAME = '{column}'"
            db.cursor.execute(query)
            return db.cursor.fetchone() is not None
        except Exception as e:
            print(f"Lỗi kiểm tra cột {column} trong bảng {table}: {e}")
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
        
        for key, value in data.items():
            if key in ['id', 'fk_id', '_id']:  # Bỏ qua id, fk_id, _id từ dữ liệu gốc
                continue
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                nested_fields[key] = value
            elif isinstance(value, dict):
                nested_fields[key] = [value]
            else:
                simple_fields[key] = convert_value_for_sql(value)

        # Thêm fk_id nếu có parent_id
        if parent_id is not None:
            simple_fields['fk_id'] = parent_id

        # Tạo câu lệnh INSERT
        if simple_fields:
            columns = [escape_column_name(safe_field_name(k)) for k in simple_fields.keys()]
            placeholders = ['?' for _ in simple_fields]
            values = list(simple_fields.values())
            
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            
            try:
                print(f"DEBUG: Đang xử lý bảng {table_name}")
                print(f"DEBUG: Các trường đơn giản: {list(simple_fields.keys())}")
                print(f"[LOG] Bảng: {table_name} | Data: {simple_fields}")
                
                db.cursor.execute(insert_sql, values)
                
                # Commit ngay lập tức để đảm bảo IDENTITY hoạt động
                db.conn.commit()
                
                # Lấy ID tự tăng từ SQL Server - thử nhiều cách
                db.cursor.execute("SELECT @@IDENTITY")
                result = db.cursor.fetchone()
                inserted_id = int(result[0]) if result and result[0] else None
                
                print(f"DEBUG: Inserted {table_name} with ID: {inserted_id}")
                print(f"DEBUG: @@IDENTITY result: {result}")
                
                # Nếu @@IDENTITY không hoạt động, thử SCOPE_IDENTITY
                if inserted_id is None:
                    db.cursor.execute("SELECT SCOPE_IDENTITY()")
                    scope_result = db.cursor.fetchone()
                    inserted_id = int(scope_result[0]) if scope_result and scope_result[0] else None
                    print(f"DEBUG: SCOPE_IDENTITY result: {scope_result}")
                
            except Exception as e:
                print(f"DEBUG: Insert failed, try update {table_name} - {e}")
                # Nếu insert thất bại, thử update
                try:
                    # Tìm record theo fk_id (nếu có) hoặc các trường khác
                    if 'fk_id' in simple_fields:
                        update_condition = f"fk_id = {simple_fields['fk_id']}"
                    else:
                        # Nếu không có fk_id, tìm theo các trường khác (trừ id)
                        conditions = []
                        for k, v in simple_fields.items():
                            if k != 'id' and v is not None:
                                if isinstance(v, str):
                                    conditions.append(f"{escape_column_name(safe_field_name(k))} = '{v}'")
                                else:
                                    conditions.append(f"{escape_column_name(safe_field_name(k))} = {v}")
                        
                        if conditions:
                            update_condition = " AND ".join(conditions)
                        else:
                            print(f"DEBUG: Không thể update {table_name} - không có điều kiện")
                            return None
                    
                    # Tạo câu lệnh UPDATE
                    set_clauses = []
                    update_values = []
                    for k, v in simple_fields.items():
                        if k != 'fk_id':  # Không update fk_id
                            set_clauses.append(f"{escape_column_name(safe_field_name(k))} = ?")
                            update_values.append(v)
                    
                    update_sql = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {update_condition}"
                    db.cursor.execute(update_sql, update_values)
                    
                    # Lấy ID của record đã update
                    select_sql = f"SELECT id FROM {table_name} WHERE {update_condition}"
                    db.cursor.execute(select_sql)
                    result = db.cursor.fetchone()
                    inserted_id = result[0] if result else None
                    
                    print(f"DEBUG: Updated {table_name} (id+fk_id) after insert fail")
                    
                except Exception as update_error:
                    print(f"DEBUG: Update also failed for {table_name}: {update_error}")
                    return None
        else:
            inserted_id = None

        # Xử lý các trường nested (bảng con)
        for field_name, nested_data in nested_fields.items():
            if not nested_data:
                continue
                
            child_table_name = f"{table_name}_{field_name}"
            
            # Kiểm tra bảng con có tồn tại không
            if not check_table_exists(child_table_name):
                print(f"DEBUG: Bảng con {child_table_name} không tồn tại, bỏ qua")
                continue
            
            print(f"DEBUG: Xử lý list {child_table_name}, số lượng: {len(nested_data)}")
            
            for i, item in enumerate(nested_data):
                print(f"DEBUG: Xử lý item {i} trong {child_table_name}")
                
                # Gọi đệ quy để insert/update vào bảng con
                child_id = upsert_recursive(item, child_table_name, inserted_id)
                
                if child_id is None:
                    print(f"DEBUG: Không thể insert/update vào {child_table_name}")

        return inserted_id

    def check_table_exists(table_name):
        try:
            query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            db.cursor.execute(query)
            return db.cursor.fetchone() is not None
        except Exception as e:
            print(f"Lỗi kiểm tra bảng {table_name}: {e}")
            return False

    try:
        # Xử lý từng customer
        for i, customer_data in enumerate(data_list, 1):
            print(f"\n=== Xử lý customer {i} ===")
            
            # Insert/update customer chính
            customer_id = upsert_recursive(customer_data, table_name)
            
            if customer_id:
                print(f"DEBUG: Customer {i} processed successfully with ID: {customer_id}")
            else:
                print(f"DEBUG: Failed to process customer {i}")

        db.conn.commit()
        print(f"\nĐã xử lý xong {len(data_list)} customer")

    except Exception as e:
        print(f"Lỗi trong quá trình insert/update: {e}")
        db.conn.rollback()
    finally:
        db.close()
