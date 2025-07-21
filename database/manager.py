import pyodbc
import pandas as pd
from database.config import DB_CONFIG, CONN_STR_TEMPLATE, DB_CONFIG_CRM, CONN_STR_TEMPLATE_CRM

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

    def update_data(self, table_name, data, where_conditions):
        """
        Cập nhật dữ liệu trong bảng
        :param table_name: Tên bảng
        :param data: Dictionary chứa dữ liệu cần cập nhật
        :param where_conditions: Dictionary chứa điều kiện WHERE
        Ví dụ: update_data('customers', {'name': 'New Name'}, {'id': 1})
        """
        try:
            # Tạo phần SET
            set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
            
            # Tạo phần WHERE
            where_clause = ' AND '.join([f"{key} = ?" for key in where_conditions.keys()])
            
            # Tạo query
            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            
            # Chuẩn bị values
            values = list(data.values()) + list(where_conditions.values())
            
            cursor = self.conn.cursor()
            cursor.execute(query, values)
            self.conn.commit()
            print(f"Đã cập nhật dữ liệu trong bảng {table_name} thành công!")
            return True
        except Exception as e:
            print(f"Lỗi cập nhật dữ liệu: {str(e)}")
            return False

    def close(self):
        """Đóng kết nối"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Đã đóng kết nối.")

class DatabaseManagerCRM:
    def __init__(self):
        self.conn_str = CONN_STR_TEMPLATE_CRM.format(**DB_CONFIG_CRM)
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print(f"Error connecting to CRM database: {str(e)}")
            return False

    def execute_query(self, query, params=None):
        try:
            if params:
                df = pd.read_sql(query, self.conn, params=params)
            else:
                df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            print(f"Lỗi thực thi query CRM: {str(e)}")
            return None

    def create_table(self, table_name: str, columns: dict) -> bool:
        try:
            if not self.connect():
                return False
            columns_str = ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])
            create_table_sql = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')\n"
            create_table_sql += f"CREATE TABLE {table_name} ({columns_str})"
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            print(f"Đã tạo bảng {table_name} trên CRM DB thành công!")
            return True
        except Exception as e:
            print(f"Error creating table on CRM DB: {str(e)}")
            return False

    def insert_data(self, table_name, data):
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            values = list(data.values())
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor = self.conn.cursor()
            cursor.execute(query, values)
            self.conn.commit()
            print(f"Đã thêm dữ liệu vào bảng {table_name} trên CRM DB thành công!")
            return True
        except Exception as e:
            print(f"Lỗi thêm dữ liệu vào CRM DB: {str(e)}")
            return False

    def update_data(self, table_name, data, where_conditions):
        try:
            set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
            where_clause = ' AND '.join([f"{key} = ?" for key in where_conditions.keys()])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            values = list(data.values()) + list(where_conditions.values())
            cursor = self.conn.cursor()
            cursor.execute(query, values)
            self.conn.commit()
            print(f"Đã cập nhật dữ liệu trong bảng {table_name} trên CRM DB thành công!")
            return True
        except Exception as e:
            print(f"Lỗi cập nhật dữ liệu CRM DB: {str(e)}")
            return False

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
