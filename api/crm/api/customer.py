from api.crm.service.fetch import call_crm_api
from api.crm.service.insert_update import insert_or_update_customer
from api.crm.service.create_table import create_table_from_object

def deep_merge_dicts(dicts):
    # Nếu đầu vào là list, merge từng dict trong list
    if isinstance(dicts, list):
        merged = {}
        all_keys = set()
        for d in dicts:
            if isinstance(d, dict):
                all_keys.update(d.keys())
        for key in all_keys:
            values = [d.get(key) for d in dicts if isinstance(d, dict) and key in d]
            non_none_values = [v for v in values if v is not None]
            if not non_none_values:
                merged[key] = None
            elif isinstance(non_none_values[0], dict):
                merged[key] = deep_merge_dicts(non_none_values)
            elif isinstance(non_none_values[0], list):
                all_list_items = []
                for value in non_none_values:
                    if isinstance(value, list):
                        all_list_items.extend(value)
                if all_list_items and all(isinstance(item, dict) for item in all_list_items):
                    merged_dict = deep_merge_dicts(all_list_items)
                    merged[key] = [merged_dict]
                else:
                    merged[key] = [all_list_items[0]] if all_list_items else []
            else:
                merged[key] = non_none_values[0]
        return merged
    # Nếu đầu vào là dict, trả về luôn
    elif isinstance(dicts, dict):
        return dicts
    else:
        return {}

def fetch_customer_data():
    path = '/_api/base-table/find'
    
    # Lấy response đầu tiên để biết total
    data = {
        "table": "data_customer",
        "limit": 1,
        "skip": 0,
        "output": "by-key"
    }
    
    first_result = call_crm_api(path, data)
    
    if not isinstance(first_result, dict) or 'total' not in first_result:
        print("Không thể lấy thông tin total từ API")
        return first_result
    
    total = first_result.get('total', 0)
    limit = 1000  # Số lượng record mỗi lần lấy
    all_data = []
    
    print(f"Tổng số customer cần lấy: {total}")
    
    # Tính số batch cần lấy
    total_batches = (total + limit - 1) // limit  # Làm tròn lên
    
    # Lấy từng batch
    for batch in range(total_batches):
        skip = batch * limit
        print(f"Đang lấy batch {batch + 1}/{total_batches} (skip: {skip})")
        
        data = {
            "table": "data_customer",
            "limit": limit,
            "skip": skip,
            "output": "by-key"
        }
        
        result = call_crm_api(path, data)
        
        if isinstance(result, dict) and 'data' in result and isinstance(result['data'], list):
            batch_data = result['data']
            all_data.extend(batch_data)
            print(f"Đã lấy được {len(batch_data)} records trong batch này")
            
            # Tạo bảng từ batch đầu tiên
            if batch == 0 and batch_data:
                merged = deep_merge_dicts(batch_data)
                result_for_table = {'data': [merged]}
                create_table_from_object(result_for_table, "crm_data_customer")
        else:
            print(f"Lỗi khi lấy batch {batch + 1}")
    
    print(f"Đã lấy được tổng cộng: {len(all_data)} records")
    
    # Insert/update toàn bộ dữ liệu
    if all_data:
        final_result = {'data': all_data}
        insert_or_update_customer(final_result)
    
    return {'data': all_data, 'total': total}
