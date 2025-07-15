DB_CONFIG = {
    'server': 'DESKTOP-9MURA5K',
    'database': 'Nhanh1',
    'username': 'sa',
    'password': 'admin'
}

# Connection string template
CONN_STR_TEMPLATE = '''DRIVER={{ODBC Driver 17 for SQL Server}};
SERVER={server};
DATABASE={database};
Trusted_Connection=yes;''' 


DB_CONFIG_CRM = {
    'server': 'DESKTOP-9MURA5K',
    'database': 'CRM',
    'username': 'sa',
    'password': 'admin'
}

CONN_STR_TEMPLATE_CRM = '''DRIVER={{ODBC Driver 17 for SQL Server}};
SERVER={server};
DATABASE={database};
UID={username};
PWD={password};''' 


# DB_CONFIG_CRM = {
#     'server': '103.61.123.57',
#     'database': 'Biz',
#     'username': 'dungth',
#     'password': '123@123a'
# }

# # Connection string template
# CONN_STR_TEMPLATE_CRM = 'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}' 


# DB_CONFIG = {
#     'server': '103.61.123.57',
#     'database': 'Nhanh',
#     'username': 'dungth',
#     'password': '123@123a'
# }

# # Connection string template
# CONN_STR_TEMPLATE = 'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}' 