# Database configuration
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