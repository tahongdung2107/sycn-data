from typing import List, Dict, Any
from database.manager import DatabaseManager

def get_sql_type(field_type: Any) -> str:
    """
    Convert field type to SQL Server data type
    """
    if isinstance(field_type, dict):
        # Handle nested object or array types
        if field_type.get('type') == 'object':
            return 'NVARCHAR(MAX)'  # Store reference to child table
        elif field_type.get('type') == 'array':
            return 'NVARCHAR(MAX)'  # Store reference to child table
        return 'NVARCHAR(255)'  # Default for unknown nested types
    
    type_mapping = {
        'int': 'NVARCHAR(50)',
        'number': 'NVARCHAR(50)',
        'string': 'NVARCHAR(255)',
        'unknown': 'NVARCHAR(255)',
        'array': 'NVARCHAR(MAX)',
        'object': 'NVARCHAR(MAX)',
        'datetime': 'NVARCHAR(50)',
        'date': 'NVARCHAR(50)',
        'text': 'NVARCHAR(MAX)',
        'boolean': 'NVARCHAR(10)'
    }
    return type_mapping.get(str(field_type).lower(), 'NVARCHAR(255)')

def create_child_table(db_manager: DatabaseManager, parent_table: str, field_name: str, field_structure: Dict[str, Any]):
    """
    Create a child table for object or array fields
    
    Args:
        db_manager: Database manager instance
        parent_table: Name of the parent table
        field_name: Name of the field
        field_structure: Structure of the field (object or array)
    """
    child_table_name = f"{parent_table}_{field_name}"
    
    # Get the structure for the child table
    if field_structure['type'] == 'object':
        fields = field_structure.get('properties', [])
    elif field_structure['type'] == 'array':
        # For arrays, we need to handle the items structure
        items = field_structure.get('items', [])
        if isinstance(items, list):
            fields = items
        else:
            # If items is a simple type, create a simple value column
            columns = {
                'id': 'NVARCHAR(50) PRIMARY KEY',
                'fk_id': 'NVARCHAR(50)',
                'value': get_sql_type(items)
            }
            try:
                success = db_manager.create_table(child_table_name, columns)
                if success:
                    print(f"Child table '{child_table_name}' created successfully!")
                return
            except Exception as e:
                print(f"Error creating child table: {str(e)}")
                return
    else:
        return

    # Convert fields to columns dictionary
    columns = {
        'id': 'NVARCHAR(50) PRIMARY KEY',
        'fk_id': 'NVARCHAR(50)'
    }
    
    for field in fields:
        field_name = field['field']
        field_type = field['type']
        columns[field_name] = get_sql_type(field_type)
    
    try:
        # Create the child table
        success = db_manager.create_table(child_table_name, columns)
        if success:
            print(f"Child table '{child_table_name}' created successfully!")
        else:
            print(f"Failed to create child table '{child_table_name}'")
    except Exception as e:
        print(f"Error creating child table: {str(e)}")

def check_column_exists(db_manager: DatabaseManager, table_name: str, column_name: str) -> bool:
    """
    Kiểm tra xem cột đã tồn tại trong bảng chưa
    
    Args:
        db_manager: Database manager instance
        table_name: Tên bảng
        column_name: Tên cột cần kiểm tra
    
    Returns:
        bool: True nếu cột tồn tại, False nếu không
    """
    try:
        if db_manager.connect():
            query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            AND COLUMN_NAME = '{column_name}'
            """
            db_manager.cursor.execute(query)
            result = db_manager.cursor.fetchone()
            return result[0] > 0
    except Exception as e:
        print(f"Lỗi khi kiểm tra cột: {str(e)}")
    return False

def create_table(table_structure: List[Dict[str, Any]], table_name: str):
    """
    Create a table using the provided table structure from Nhanh API
    
    Args:
        table_structure (List[Dict]): List of field definitions from Nhanh API response
        table_name (str): Name of the table to create
    """
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Convert fields to columns dictionary
        columns = {}
        nested_fields = []  # Store fields that need child tables
        
        for field in table_structure:
            field_name = field['field']
            field_type = field['type']
            
            # Add PRIMARY KEY constraint for 'id' field
            if field_name == 'id':
                columns[field_name] = 'NVARCHAR(50) PRIMARY KEY'
            else:
                columns[field_name] = get_sql_type(field_type)
                
                # Store nested fields for child table creation
                if isinstance(field_type, dict) and field_type.get('type') in ['object', 'array']:
                    nested_fields.append((field_name, field_type))

        # Kiểm tra xem bảng đã tồn tại chưa
        if db_manager.connect():
            check_table_query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table_name}'
            """
            db_manager.cursor.execute(check_table_query)
            table_exists = db_manager.cursor.fetchone()[0] > 0

            if not table_exists:
                # Tạo bảng mới nếu chưa tồn tại
                success = db_manager.create_table(table_name, columns)
                if success:
                    print(f"Bảng '{table_name}' đã được tạo thành công!")
                else:
                    print(f"Không thể tạo bảng '{table_name}'")
                    return
            else:
                # Thêm các cột mới vào bảng đã tồn tại
                for column_name, column_type in columns.items():
                    if not check_column_exists(db_manager, table_name, column_name):
                        alter_query = f"""
                        ALTER TABLE {table_name}
                        ADD {column_name} {column_type}
                        """
                        try:
                            db_manager.cursor.execute(alter_query)
                            db_manager.conn.commit()
                            print(f"Đã thêm cột '{column_name}' vào bảng '{table_name}'")
                        except Exception as e:
                            print(f"Lỗi khi thêm cột '{column_name}': {str(e)}")
            
            # Create child tables for nested fields
            for field_name, field_type in nested_fields:
                create_child_table(db_manager, table_name, field_name, field_type)

    except Exception as e:
        print(f"Lỗi khi tạo bảng: {str(e)}")
    finally:
        if 'db_manager' in locals():
            db_manager.close()

def create_table_from_array(table_name: str, fields_array: List[Dict[str, str]]):
    """
    Create a table from an array of field definitions
    
    Args:
        table_name (str): Name of the table to create
        fields_array (List[Dict[str, str]]): List of field definitions where each field is a dict with field name as key and type as value
        Example: [{'id': 'int'}, {'name': 'string'}, {'age': 'number'}]
    """
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Convert fields to columns dictionary
        columns = {}
        for field_dict in fields_array:
            for field_name, field_type in field_dict.items():
                sql_type = get_sql_type(field_type)
                
                # Add PRIMARY KEY constraint for 'id' field
                if field_name == 'id':
                    columns[field_name] = f"{sql_type} PRIMARY KEY"
                else:
                    columns[field_name] = sql_type

        # Create table using database manager
        success = db_manager.create_table(table_name, columns)
        
        if success:
            print(f"Table '{table_name}' created successfully!")
        else:
            print(f"Failed to create table '{table_name}'")

    except Exception as e:
        print(f"Error creating table: {str(e)}")
    finally:
        if 'db_manager' in locals():
            db_manager.close()

