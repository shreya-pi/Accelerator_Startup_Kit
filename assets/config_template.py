# --- Snowflake Connection Details ---
# Provide your Snowflake account details here.
SNOWFLAKE_CONFIG = {
    "user": "your_snowflake_user",
    "password": "your_snowflake_password",
    "account": "your_account_identifier", 
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "YOUR_SCHEMA",
    "role": "YOUR_ROLE"
}

# --- SQL Server Connection Details ---
# Provide your SQL Server details.
# The driver name might vary based on your OS and installation.
# Common drivers: 'ODBC Driver 17 for SQL Server' or '{SQL Server}'.
SQL_SERVER_CONFIG = {
    "driver": "{ODBC Driver 17 for SQL Server}",
    "server": r"your_server_name_or_ip",
    "database": "your_source_database",
    "username": "your_sql_server_user",
    "password": r"your_sql_server_password"
}