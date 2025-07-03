DB_CONFIG = {
    'server': 'DESKTOP-9MURA5K',
    'database': 'Nhanh',
    'username': 'sa',
    'password': 'admin'
}

# Connection string template
CONN_STR_TEMPLATE = '''DRIVER={{ODBC Driver 17 for SQL Server}};
SERVER={server};
DATABASE={database};
Trusted_Connection=yes;''' 


# DB_CONFIG = {
#     'server': '14.177.129.7',
#     'database': 'Nhanh',
#     'username': 'dungth',
#     'password': 'admin'
# }

# # Connection string template
# CONN_STR_TEMPLATE = 'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}' 