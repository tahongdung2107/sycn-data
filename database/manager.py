import pyodbc
import pandas as pd
from database.config import DB_CONFIG, CONN_STR_TEMPLATE

class DatabaseManager:
    def __init__(self):
        self.conn_str = CONN_STR_TEMPLATE.format(**DB_CONFIG)
        self.conn = None
        self.cursor = None

    def connect(self):
        """Kết nối đến SQL Server"""
        try:
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            print("Kết nối thành công đến SQL Server!")
            return True
        except Exception as e:
            print(f"Error connecting to database: {str(e)}")
            return False

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

    def create_table(self, table_name: str, columns: dict) -> bool:
        """
        Create a table with the specified columns
        
        Args:
            table_name (str): Name of the table to create
            columns (dict): Dictionary of column names and their SQL types
            
        Returns:
            bool: True if table was created successfully, False otherwise
        """
        try:
            if not self.connect():
                return False

            # Create the CREATE TABLE statement
            columns_str = ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])
            create_table_sql = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')\n"
            create_table_sql += f"CREATE TABLE {table_name} ({columns_str})"
            
            # Execute the CREATE TABLE statement
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            print(f"Đã tạo bảng {table_name} thành công!")
            return True

        except Exception as e:
            print(f"Error creating table: {str(e)}")
            return False
        finally:
            self.close()

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
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Đã đóng kết nối.")