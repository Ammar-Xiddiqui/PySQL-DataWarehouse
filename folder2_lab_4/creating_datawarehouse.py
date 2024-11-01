import pyodbc

server_name = 'DESKTOP-G4025QF\\AMMAR'  # Replace with your server name
database_name = 'northwind3'

try:
    with pyodbc.connect(
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={server_name};Trusted_Connection=yes;",
        autocommit=True  
    ) as conn:
        
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT name FROM master.dbo.sysdatabases WHERE name = '{database_name}'")
            existing_db = cursor.fetchone()

            if existing_db:
                print(f"Database '{database_name}' already exists.")
            else:
                cursor.execute(f"CREATE DATABASE {database_name}")
                print(f"Database '{database_name}' created successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
