from api.crm.service.fetch import call_crm_api
from api.crm.service.create_table import create_table_from_object

def fetch_customer_data():
    path = '/_api/base-table/find'
    data = {
        "table": "data_customer",
        "limit": 1,
        "skip": 0,
        "output": "by-key"
    }
    result = call_crm_api(path, data)
    # Lấy object đầu tiên của data (chuẩn theo response thực tế)
    data_obj = None
    if isinstance(result, dict) and 'data' in result and isinstance(result['data'], list) and result['data']:
        data_obj = result['data'][0]
    if data_obj:
        create_table_from_object(data_obj, "crm_data_customer")
    return result
