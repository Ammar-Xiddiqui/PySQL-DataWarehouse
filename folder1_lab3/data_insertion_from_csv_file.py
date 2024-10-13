import pyodbc
import pandas as pd


conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-G4025QF\\AMMAR;"  # update with ur server name
    "Database=northwind;"  
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()
# ----------------------------------inserting by csv files ------------------



# -------------------------------------inserting table name categories----

file_path = 'C:\\Users\\Ammar\\Desktop\\lab3dw\\northwind\\Categories.csv'  # file path 
df = pd.read_csv(file_path)

# removing spaces 
df.columns = df.columns.str.strip()

# for empty string 
df['CategoryName'].replace('', None, inplace=True)
df['Description'].replace('', None, inplace=True)

# check if CategoryID is missing or not
if df['CategoryID'].isnull().any():
    print("missing data found in categoryid. dropping that rows")
    df = df.dropna(subset=['CategoryID'])  

# CategoryID is an integer
df['CategoryID'] = df['CategoryID'].astype(int)

try:
    for index, row in df.iterrows():
        category_id = row['CategoryID']
        category_name = row['CategoryName']
        description = row['Description']
        
        #if category already exists
        cursor.execute("SELECT COUNT(*) FROM Categories WHERE CategoryID = ?", category_id)
        if cursor.fetchone()[0] > 0:
            print(f"category {category_id} exists. skipping insert")
            continue


        cursor.execute('''
            INSERT INTO Categories (CategoryID, CategoryName, Description, Picture) 
            VALUES (?, ?, ?, NULL)
            ''', category_id, category_name, description)

    conn.commit()
    print("data inserted")

except Exception as e:
    print(f"an error happened: {e}")
    conn.rollback()  

# -------------------------------------inserting table name customer--------------------

file_path = 'C:\\Users\\Ammar\\Desktop\\lab3dw\\northwind\\Customers.csv'  # file path
try:
    #skip bad lines
    df = pd.read_csv(file_path, on_bad_lines='skip')

    
    df.columns = df.columns.str.strip()

    df['CustomerName'].replace('', None, inplace=True)
    df['ContactName'].replace('', None, inplace=True)
    df['Address'].replace('', None, inplace=True)
    df['City'].replace('', None, inplace=True)
    df['PostalCode'].replace('', None, inplace=True)
    df['Country'].replace('', None, inplace=True)

    if df['CustomerID'].isnull().any():
        print("missing values in customerid. removing those rows")
        df = df.dropna(subset=['CustomerID'])  

    # make sure customerid is int
    df['CustomerID'] = df['CustomerID'].astype(int)

    for index, row in df.iterrows():
        customer_id = row['CustomerID']
        company_name = row['CustomerName']  # assuming this is the companyname
        contact_name = row['ContactName']
        address = row['Address']
        city = row['City']
        postal_code = row['PostalCode']
        country = row['Country']

        # if customer already exists
        cursor.execute("SELECT COUNT(*) FROM Customers WHERE CustomerID = ?", customer_id)
        if cursor.fetchone()[0] > 0:
            print(f"customer {customer_id} exists. skipping insert")
            continue

        cursor.execute('''
            INSERT INTO Customers (CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax) 
            VALUES (?, ?, ?, NULL, ?, ?, NULL, ?, ?, NULL, NULL)
            ''', customer_id, company_name, contact_name, address, city, postal_code, country)

    conn.commit()
    print("data inserted successfully")

except Exception as e:
    print(f"something went wrong: {e}")
    conn.rollback()

# ---------------------------------- inserting table name shipper--------------------

file_path = 'C:\\Users\\Ammar\\Desktop\\lab3dw\\northwind\\Shippers.csv'
try:
    df = pd.read_csv(file_path, on_bad_lines='skip')

    df.columns = df.columns.str.strip()

    df['ShipperID'] = df['ShipperID'].replace('', None)
    df['CompanyName'] = df['CompanyName'].replace('', None)
    df['Phone'] = df['Phone'].replace('', None)

    if df['ShipperID'].isnull().any():
        print("missing shipperid. dropping those rows")
        df = df.dropna(subset=['ShipperID'])  # drop rows missing shipperid

    df['ShipperID'] = df['ShipperID'].astype(int)

    for index, row in df.iterrows():
        shipper_id = int(row['ShipperID'])  
        company_name = row['CompanyName']
        phone = row['Phone']

        cursor.execute("SELECT COUNT(*) FROM Shippers WHERE ShipperID = ?", shipper_id)
        if cursor.fetchone()[0] > 0:
            print(f"shipper {shipper_id} exists. skipping insert")
            continue

        cursor.execute('''
            INSERT INTO Shippers (ShipperID, CompanyName, Phone) 
            VALUES (?, ?, ?)
            ''', shipper_id, company_name, phone)

    conn.commit()
    print("data inserted successfully")

except Exception as e:
    print(f"error found: {e}")
    conn.rollback()  


# -------------------------------- inserting table name product ---------------------------------
file_path = 'C:\\Users\\Ammar\\Desktop\\lab3dw\\northwind\\Products.csv'
try:
    df = pd.read_csv(file_path, on_bad_lines='skip')

    df.columns = df.columns.str.strip()

    # rename some columns to match the db table
    df.rename(columns={'Unit': 'QuantityPerUnit', 'Price': 'UnitPrice'}, inplace=True)

    df['ProductID'] = df['ProductID'].replace('', None)
    df['ProductName'] = df['ProductName'].replace('', None)
    df['SupplierID'] = df['SupplierID'].replace('', None)
    df['CategoryID'] = df['CategoryID'].replace('', None)
    df['QuantityPerUnit'] = df['QuantityPerUnit'].replace('', None)
    df['UnitPrice'] = df['UnitPrice'].replace('', None)

    # check for missing values in 'ProductID'
    if df['ProductID'].isnull().any():
        print("found missing values in 'productID'. dropping those rows.")
        df = df.dropna(subset=['ProductID'])  

    df['ProductID'] = df['ProductID'].astype(int)
    df['SupplierID'] = df['SupplierID'].astype(int)
    df['UnitPrice'] = df['UnitPrice'].astype(float)

    for index, row in df.iterrows():
        product_id = int(row['ProductID']) 
        product_name = row['ProductName']
        supplier_id = int(row['SupplierID']) 
        category_id = int(row['CategoryID']) if pd.notnull(row['CategoryID']) else None 
        quantity_per_unit = row['QuantityPerUnit']
        unit_price = row['UnitPrice']

        # check if supplierid and categoryid exist to avoid errors
        cursor.execute("SELECT COUNT(*) FROM Suppliers WHERE SupplierID = ?", supplier_id)
        if cursor.fetchone()[0] == 0:
            print(f"supplierid {supplier_id} doesn't exist. skipping productid {product_id}.")
            continue
        
        if category_id is not None:
            cursor.execute("SELECT COUNT(*) FROM Categories WHERE CategoryID = ?", category_id)
            if cursor.fetchone()[0] == 0:
                print(f"categoryid {category_id} doesn't exist. skipping productid {product_id}.")
                continue

        # insert into the products table
        cursor.execute('''
            INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued) 
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0)
            ''', product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price)

    conn.commit()
    print("data inserted good!")

except Exception as e:
    print(f"an error happened: {e}")
    conn.rollback()  

# -------------------------------- inserting table name orders -------------------------------
file_path = 'C:\\Users\\Ammar\\Desktop\\lab3dw\\northwind\\Orders.csv'
try:

    df = pd.read_csv(file_path, on_bad_lines='skip')

    df.columns = df.columns.str.strip()

    df['OrderID'] = df['OrderID'].replace('', None)
    df['CustomerID'] = df['CustomerID'].replace('', None)
    df['EmployeeID'] = df['EmployeeID'].replace('', None)
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')  # change to date
    df['ShipperID'] = df['ShipperID'].replace('', None)

    if df['OrderID'].isnull().any():
        print("found missing orderid. dropping rows.")
        df = df.dropna(subset=['OrderID'])

    df['OrderID'] = df['OrderID'].astype(int)

    for index, row in df.iterrows():
        order_id = int(row['OrderID']) 
        customer_id = int(row['CustomerID']) if pd.notnull(row['CustomerID']) else None  
        employee_id = int(row['EmployeeID']) if pd.notnull(row['EmployeeID']) else None  
        order_date = row['OrderDate']
        shipper_id = int(row['ShipperID']) if pd.notnull(row['ShipperID']) else None  # handle missing shipper id

        # check if the orderid already exists
        cursor.execute("SELECT COUNT(*) FROM Orders WHERE OrderID = ?", order_id)
        if cursor.fetchone()[0] > 0:
            print(f"orderid {order_id} already exists. skipping.")
            continue

        # check if customerid and shipperid exist to avoid problems
        if customer_id is not None:
            cursor.execute("SELECT COUNT(*) FROM Customers WHERE CustomerID = ?", customer_id)
            if cursor.fetchone()[0] == 0:
                print(f"customerid {customer_id} not found. skipping orderid {order_id}.")
                continue

        if shipper_id is not None:
            cursor.execute("SELECT COUNT(*) FROM Shippers WHERE ShipperID = ?", shipper_id)
            if cursor.fetchone()[0] == 0:
                print(f"shipperid {shipper_id} not found. skipping orderid {order_id}.")
                continue

        # insert into orders table
        cursor.execute('''
            INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipVia) 
            VALUES (?, ?, ?, ?, ?)
            ''', order_id, customer_id, employee_id, order_date, shipper_id)

    conn.commit()
    print("data inserted")

except Exception as e:
    print(f"an error happened: {e}")
    conn.rollback()  


# -------------------------- inserting table name orderdetails ---------------------------

file_path = 'C:\\Users\\Ammar\\Desktop\\lab3dw\\northwind\\Order_details.csv'

try:
    df = pd.read_csv(file_path, on_bad_lines='skip')

    df.columns = df.columns.str.strip()

    df['OrderID'] = df['OrderID'].replace('', None)
    df['ProductID'] = df['ProductID'].replace('', None)
    df['Quantity'] = df['Quantity'].replace('', None)

    if df['OrderID'].isnull().any() or df['ProductID'].isnull().any():
        print("found missing 'orderid' or 'productid'. dropping rows.")
        df = df.dropna(subset=['OrderID', 'ProductID'])  

    df['OrderID'] = df['OrderID'].astype(int)
    df['ProductID'] = df['ProductID'].astype(int)
    df['Quantity'] = df['Quantity'].astype(int)

    for index, row in df.iterrows():
        order_id = int(row['OrderID'])  # convert to int
        product_id = int(row['ProductID'])  # convert to int
        quantity = int(row['Quantity'])  # convert to int

        # check if the orderid-productid combo already exists
        cursor.execute("SELECT COUNT(*) FROM OrderDetails WHERE OrderID = ? AND ProductID = ?", order_id, product_id)
        if cursor.fetchone()[0] > 0:
            print(f"orderid {order_id} and productid {product_id} already exist. skipping.")
            continue

        # insert into the orderdetails table
        cursor.execute('''
            INSERT INTO OrderDetails (OrderID, ProductID, UnitPrice, Quantity, Discount) 
            VALUES (?, ?, NULL, ?, NULL)
            ''', order_id, product_id, quantity)

    conn.commit()
    print("data inserted")

except Exception as e:
    print(f"an error happened: {e}")
    conn.rollback()  

finally:
    conn.commit()
    cursor.close()
    conn.close()

