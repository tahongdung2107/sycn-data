from database.manager import DatabaseManagerCRM
from typing import Dict, Any

def create_table_from_object(obj: Dict[str, Any], table_name: str):
    """
    Tạo bảng chính và các bảng phụ (nếu có nested list object), bảng phụ có trường fk_id. Chỉ dùng cú pháp SQL Server.
    """
    def escape_column_name(column_name: str) -> str:
        reserved_keywords = {
            'order', 'group', 'select', 'from', 'where', 'join', 'left', 'right', 'inner', 'outer',
            'having', 'by', 'desc', 'asc', 'top', 'distinct', 'union', 'all', 'insert', 'update',
            'delete', 'create', 'drop', 'alter', 'table', 'view', 'index', 'primary', 'foreign',
            'key', 'references', 'constraint', 'default', 'check', 'unique', 'null', 'not', 'and',
            'or', 'in', 'exists', 'between', 'like', 'is', 'as', 'on', 'using', 'natural', 'cross'
        }
        return f'[{column_name}]' if column_name.lower() in reserved_keywords else f'[{column_name}]'

    def get_sql_type(value: Any) -> str:
        # Mapping đơn giản cho SQL Server
        return 'NVARCHAR(MAX)'

    def create_table(db_manager, table, sample_obj, parent=False):
        columns = []
        for k, v in sample_obj.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                continue
            elif isinstance(v, dict):
                columns.append(f'{escape_column_name(k)} {get_sql_type(v)}')
            elif k == 'id':
                columns.append(f'{escape_column_name(k)} NVARCHAR(255) PRIMARY KEY')
            else:
                columns.append(f'{escape_column_name(k)} {get_sql_type(v)}')
        if parent:
            columns.append(f'{escape_column_name("fk_id")} NVARCHAR(255)')
        col_str = ', '.join(columns)
        sql = f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table}')
BEGIN
    CREATE TABLE {escape_column_name(table)} ({col_str})
END
"""
        db_manager.cursor.execute(sql)

    def handle_nested(db_manager, table, obj, parent=False):
        if not isinstance(obj, dict):
            return
        create_table(db_manager, table, obj, parent)
        for k, v in obj.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                sub_table = f'{table}_{k}'
                handle_nested(db_manager, sub_table, v[0], parent=True)

    db_manager = DatabaseManagerCRM()
    try:
        if db_manager.connect():
            if 'data' in obj and isinstance(obj['data'], list) and obj['data']:
                handle_nested(db_manager, table_name, obj['data'][0], parent=False)
            elif isinstance(obj, dict):
                handle_nested(db_manager, table_name, obj, parent=False)
            db_manager.conn.commit()
    finally:
        db_manager.close()
