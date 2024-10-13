# -------------runnuing differernt queries on non primary key index and check their execution times and then plot times on graph-------------------------- 

import pyodbc
import time
import matplotlib.pyplot as plt

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-G4025QF\\AMMAR;"  
    "Database=northwind;"             
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Queries on Non-PK indexes
queries_non_pk = [
    ("SELECT * FROM Customers WHERE CompanyName = ?", ('Cornershop',)),  
    ("SELECT * FROM Shippers WHERE CompanyName = ?", ('Fruitful Farms',)),    
    ("SELECT * FROM Orders WHERE OrderDate = ?", ('2022-12-07',)),            
    ("SELECT o.* FROM Orders o JOIN Customers c ON o.CustomerID = c.CustomerID WHERE c.CompanyName = ?", ('Ming Li',)),  # Orders by CustomerName
    ("SELECT o.* FROM Orders o JOIN Shippers s ON o.ShipVia = s.ShipperID WHERE s.CompanyName = ?", ('Maria Anders',))  
]

execution_times_non_pk = []

query_number = 1
for query, params in queries_non_pk:
    start_time = time.perf_counter()
    cursor.execute(query, params)
    results = cursor.fetchall()
    end_time = time.perf_counter()

    execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
    execution_times_non_pk.append(execution_time)

    print(f"Query {query_number} results: {results}")
    print(f"Query {query_number} execution time: {execution_time:10f} ms")

    query_number += 1  

cursor.close()
conn.close()

# Plotting the execution time
plt.bar([f'Q{i}' for i in range(1, 6)], execution_times_non_pk, color='lightgreen')
plt.xlabel('Queries (Non-PK Indexes)')
plt.ylabel('Execution Time (ms)')
plt.title('Execution Time of Queries (Non-PK Indexes)')
plt.show()
