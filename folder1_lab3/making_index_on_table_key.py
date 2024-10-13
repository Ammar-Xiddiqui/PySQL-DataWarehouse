import pyodbc

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-G4025QF\\AMMAR;"  # Replace server name
    "Database=northwind;"               
    "Trusted_Connection=yes;"
)


cursor = conn.cursor()

# List of indexes to create
indexes = [
    ("Orders", "OrderID", "PK_Orders_OrderID"),
    ("Customers", "CustomerID", "PK_Customers_CustomerID"),
    ("Suppliers", "SupplierID", "PK_Suppliers_SupplierID"),
    ("Products", "ProductID", "PK_Products_ProductID"),
    ("Employees", "EmployeeID", "PK_Employees_EmployeeID"),
    ("Customers", "CustomerName", "IDX_Customers_CustomerName"),
    ("Shippers", "CompanyName", "IDX_Shippers_CompanyName"),
    ("Orders", "OrderDate", "IDX_Orders_OrderDate")
]

# Create indexes 
for table, column, index_name in indexes:
    create_index_query = f"CREATE INDEX {index_name} ON {table}({column});"
    cursor.execute(create_index_query)
    print(f"index {index_name} created on {table}({column}).")

conn.commit()

cursor.close()
conn.close()
