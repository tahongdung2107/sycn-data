import json
from service.createTable import create_table

def analyze_object_to_table_structure(object_data: dict) -> list:
    """
    Chuyển object mẫu thành table structure dạng list[dict] cho create_table
    """
    fields = []
    for key, value in object_data.items():
        field_type = None
        if isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], dict):
                # Nested array of object
                field_type = {'type': 'array', 'items': analyze_object_to_table_structure(value[0])}
            else:
                field_type = 'array'
        elif isinstance(value, dict):
            field_type = {'type': 'object', 'properties': analyze_object_to_table_structure(value)}
        elif isinstance(value, int):
            field_type = 'int'
        elif isinstance(value, float):
            field_type = 'number'
        elif isinstance(value, bool):
            field_type = 'boolean'
        else:
            field_type = 'string'
        fields.append({'field': key, 'type': field_type})
    return fields

def create_table_from_object(object_data: dict, table_name: str):
    """
    Phân tích object_data thành table structure và gọi create_table để tạo bảng thật
    """
    table_structure = analyze_object_to_table_structure(object_data)
    create_table(table_structure, table_name)
