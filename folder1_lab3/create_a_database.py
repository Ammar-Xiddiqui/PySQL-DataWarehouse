import pyodbc #library to connect with server

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-G4025QF\\AMMAR;" #write your server name here
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Create database
database_name = 'northwind'
try: #better to write in this format of try-except to handle any possible error
    cursor.execute(f"CREATE DATABASE {database_name}")
    print(f"Database '{database_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Close connection to prevent any data loss or face any security issue
conn.commit()
cursor.close()
conn.close()
