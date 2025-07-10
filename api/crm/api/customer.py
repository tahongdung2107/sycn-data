from api.crm.service.fetch import call_crm_api
from api.crm.service.insert_update import insert_or_update_customer
from api.crm.service.create_table import create_table_from_object

def deep_merge_dicts(dicts):
    merged = {}
    # Thu thập tất cả các key có thể có
    all_keys = set()
    for d in dicts:
        all_keys.update(d.keys())
    
    for key in all_keys:
        values = [d.get(key) for d in dicts if key in d]
        non_none_values = [v for v in values if v is not None]
        
        if not non_none_values:
            merged[key] = None
        elif isinstance(non_none_values[0], dict):
            # Merge các dict
            merged[key] = deep_merge_dicts(non_none_values)
        elif isinstance(non_none_values[0], list):
            # Xử lý list
            all_list_items = []
            for value in non_none_values:
                if isinstance(value, list):
                    all_list_items.extend(value)
            
            if all_list_items and all(isinstance(item, dict) for item in all_list_items):
                # Nếu là list các dict, merge tất cả các dict
                merged_dict = deep_merge_dicts(all_list_items)
                merged[key] = [merged_dict]
            else:
                # Nếu là list thường, lấy item đầu tiên
                merged[key] = [all_list_items[0]] if all_list_items else []
        else:
            # Các kiểu dữ liệu khác, lấy giá trị đầu tiên
            merged[key] = non_none_values[0]
    
    return merged

def fetch_customer_data():
    path = '/_api/base-table/find'
    data = {
        "table": "data_customer",
        "limit": 500,
        "skip": 0,
        "output": "by-key"
    }
    result = call_crm_api(path, data)
    
    # Merge sâu tất cả các trường của các item trong data
    if isinstance(result, dict) and 'data' in result and isinstance(result['data'], list) and result['data']:
        merged = deep_merge_dicts(result['data'])
        result_for_table = {'data': [merged]}
        create_table_from_object(result_for_table, "crm_data_customer")
    else:
        create_table_from_object(result, "crm_data_customer")
    
    # Nếu có data thì insert/update toàn bộ
    if isinstance(result, dict) and 'data' in result and isinstance(result['data'], list) and result['data']:
        insert_or_update_customer(result)
    return result
