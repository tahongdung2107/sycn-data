from api.crm.service.fetch import call_crm_api
from api.crm.service.insert_update_customer_final import insert_or_update_customer_final
from api.crm.service.create_table_customer import create_table_from_object
import datetime

def fetch_customer_data(start_date=None, end_date=None):
    path = '/_api/base-table/find'
    # Nếu không truyền ngày thì lấy ngày hôm qua
    if not start_date or not end_date:
        today = datetime.datetime.now()
        start_str = today.strftime('%Y-%m-%dT00:00:00.000Z')
        end_str = today.strftime('%Y-%m-%dT23:59:59.999Z')
        # start_str = datetime.datetime(2024, 1, 1).strftime('%Y-%m-%dT00:00:00.000Z')
        # end_str = datetime.datetime.now().strftime('%Y-%m-%dT23:59:59.999Z')
    else:
        start_str = start_date
        end_str = end_date
    data = {
        "table": "data_customer",
        "limit": 1,
        "skip": 0,
        "output": "by-key",
        "query": {
            "updated_at": {
                "$gte": start_str,
                "$lte": end_str
            }
        }
    }
    # Lấy response đầu tiên để biết total
    first_result = call_crm_api(path, data)
    if not isinstance(first_result, dict) or 'total' not in first_result:
        print("Không thể lấy thông tin total từ API")
        return first_result
    total = first_result.get('total', 0)
    limit = 1000
    all_data = []
    print(f"Tổng số customer cần lấy: {total}")
    total_batches = (total + limit - 1) // limit
    for batch in range(total_batches):
        skip = batch * limit
        print(f"Đang lấy batch {batch + 1}/{total_batches} (skip: {skip})")
        data = {
            "table": "data_customer",
            "limit": limit,
            "skip": skip,
            "output": "by-key",
            "query": {
                "updated_at": {
                    "$gte": start_str,
                    "$lte": end_str
                }
            }
        }
        result = call_crm_api(path, data)
        if isinstance(result, dict) and 'data' in result and isinstance(result['data'], list):
            batch_data = result['data']
            all_data.extend(batch_data)
            print(f"Đã lấy được {len(batch_data)} records trong batch này")
            # Tạo bảng từ batch đầu tiên
            if batch == 0 and batch_data:
                merged = batch_data[0]
                result_for_table = {'data': [merged]}
                print("Tạo bảng từ batch đầu tiên...")
                create_table_from_object(result_for_table, "crm_data_customer")
        else:
            print(f"Lỗi khi lấy batch {batch + 1}")
    print(f"Đã lấy được tổng cộng: {len(all_data)} records")
    # Insert/update toàn bộ dữ liệu
    if all_data:
        final_result = {'data': all_data}
        print("Bắt đầu insert/update dữ liệu vào database...")
        insert_or_update_customer_final(final_result, table_name="crm_data_customer")
        print("Đã insert/update xong dữ liệu vào database!")
    return {'data': all_data, 'total': total}
