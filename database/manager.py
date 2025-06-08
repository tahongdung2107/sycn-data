import pyodbc
import pandas as pd
from config import DB_CONFIG, CONN_STR_TEMPLATE

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        """Kết nối đến SQL Server"""
        try:
            conn_str = CONN_STR_TEMPLATE.format(**DB_CONFIG)
            self.conn = pyodbc.connect(conn_str)
            print("Kết nối thành công đến SQL Server!")
        except Exception as e:
            print(f"Lỗi kết nối: {str(e)}")
            self.conn = None

    def execute_query(self, query, params=None):
        """Thực thi query và trả về kết quả dạng DataFrame"""
        try:
            if params:
                df = pd.read_sql(query, self.conn, params=params)
            else:
                df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            print(f"Lỗi thực thi query: {str(e)}")
            return None

    def create_table(self, table_name, columns):
        """
        Tạo bảng mới
        :param table_name: Tên bảng
        :param columns: Dictionary chứa tên cột và kiểu dữ liệu
        Ví dụ: {'id': 'INT PRIMARY KEY', 'name': 'NVARCHAR(100)'}
        """
        try:
            columns_str = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
            query = f"CREATE TABLE {table_name} ({columns_str})"
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()
            print(f"Đã tạo bảng {table_name} thành công!")
            return True
        except Exception as e:
            print(f"Lỗi tạo bảng: {str(e)}")
            return False

    def insert_data(self, table_name, data):
        """
        Thêm dữ liệu vào bảng
        :param table_name: Tên bảng
        :param data: Dictionary chứa dữ liệu cần thêm
        Ví dụ: {'id': 1, 'name': 'Test'}
        """
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            values = list(data.values())
            
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            cursor = self.conn.cursor()
            cursor.execute(query, values)
            self.conn.commit()
            print(f"Đã thêm dữ liệu vào bảng {table_name} thành công!")
            return True
        except Exception as e:
            print(f"Lỗi thêm dữ liệu: {str(e)}")
            return False

    def close(self):
        """Đóng kết nối"""
        if self.conn:
            self.conn.close()
            print("Đã đóng kết nối.")