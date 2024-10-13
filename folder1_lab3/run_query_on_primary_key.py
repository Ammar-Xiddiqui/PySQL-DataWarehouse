# -------------runnuing differernt queries on primary key index and check their execution times and then plot times on graph-------------------------- 
import pyodbc
import time
import matplotlib.pyplot as plt

# Database connection
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-G4025QF\\AMMAR;"  
    "Database=northwind;"             
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Queries
queries = [
    ("SELECT * FROM Orders WHERE OrderID = ?", (10255,)),  
    ("SELECT * FROM Customers WHERE CustomerID = ?", (7,)),  
    ("SELECT * FROM Suppliers WHERE SupplierID = ?", (1,)),  
    ("SELECT * FROM Products WHERE ProductID = ?", (1,)),    
    ("SELECT * FROM Employees WHERE EmployeeID = ?", (1,))   
]

execution_times = []

# Run each query and store its execution time
query_number = 1
for query, params in queries:
    start_time = time.perf_counter()
    cursor.execute(query, params)
    results = cursor.fetchall()
    end_time = time.perf_counter()

    execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
    execution_times.append(execution_time)

    print(f"Query {query_number} results: {results}")
    print(f"Query {query_number} execution time: {execution_time:10f} ms")

    query_number += 1  


# Close connection
cursor.close()
conn.close()

# Plotting the execution times
plt.bar([f'Q{i}' for i in range(1, 6)], execution_times, color='skyblue')
plt.xlabel('Queries')
plt.ylabel('Execution Time (ms)')
plt.title('Execution Time of Queries')
plt.show()
