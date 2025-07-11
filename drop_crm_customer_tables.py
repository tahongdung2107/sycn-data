from database.manager import DatabaseManager

def drop_all_crm_customer_tables():
    db = DatabaseManager()
    db.connect()
    # Lấy danh sách tất cả các bảng bắt đầu bằng 'crm_data_customer_data'
    db.cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE 'crm_data_%'
        ORDER BY TABLE_NAME DESC
    """)
    tables = [row[0] for row in db.cursor.fetchall()]
    print("Các bảng sẽ bị xóa:", tables)
    for table in tables:
        try:
            db.cursor.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Đã xóa bảng {table}")
        except Exception as e:
            print(f"Lỗi khi xóa bảng {table}: {e}")
    db.conn.commit()
    db.close()
    print("Đã xóa xong toàn bộ các bảng liên quan.")

if __name__ == "__main__":
    drop_all_crm_customer_tables()