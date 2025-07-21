from database.manager import DatabaseManagerCRM

# Xóa tất cả các bảng bắt đầu bằng 'crm_data_customer'
def delete_crm_customer_tables():
    db = DatabaseManagerCRM()
    if not db.connect():
        print("Không thể kết nối database")
        return
    try:
        # Lấy danh sách bảng cần xóa
        db.cursor.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE 'crm_data_customer%'
        """)
        tables = [row[0] for row in db.cursor.fetchall()]
        if not tables:
            print("Không có bảng nào để xóa!")
            return
        # Tắt kiểm tra khóa ngoại
        db.cursor.execute("EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'")
        # Xóa từng bảng
        for table in tables:
            try:
                db.cursor.execute(f"DROP TABLE {table}")
                print(f"Đã xóa bảng: {table}")
            except Exception as e:
                print(f"Lỗi khi xóa bảng {table}: {e}")
        db.conn.commit()
    except Exception as e:
        print(f"Lỗi khi lấy danh sách bảng: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    delete_crm_customer_tables()
