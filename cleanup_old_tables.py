from database.manager import DatabaseManagerCRM

def cleanup_old_tables():
    """Xóa các bảng cũ nếu cần"""
    db_manager = DatabaseManagerCRM()
    try:
        if db_manager.connect():
            # Xóa bảng chính nếu tồn tại
            print("Đang xóa bảng crm_data_customer nếu tồn tại...")
            db_manager.cursor.execute("IF OBJECT_ID('crm_data_customer', 'U') IS NOT NULL DROP TABLE crm_data_customer")
            
            # Xóa các bảng phụ nếu tồn tại
            print("Đang xóa các bảng phụ nếu tồn tại...")
            db_manager.cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE N'crm_data_customer_%'")
            sub_tables = db_manager.cursor.fetchall()
            
            for table in sub_tables:
                table_name = table[0]
                print(f"  Xóa bảng: {table_name}")
                db_manager.cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
            
            db_manager.conn.commit()
            print(f"Đã xóa {len(sub_tables)} bảng phụ")
            print("Hoàn thành cleanup!")
            
    finally:
        db_manager.close()

if __name__ == "__main__":
    cleanup_old_tables() 