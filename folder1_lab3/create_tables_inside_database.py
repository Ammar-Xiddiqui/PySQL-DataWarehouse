import pyodbc

# Database connection
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-G4025QF\\AMMAR;"  # Update with your server name
    "Database=northwind;"  # Ensure you're connected to the Northwind database
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# SQL queries write in an array , soe we can execute them sequentially
queries = [
    """
    CREATE TABLE Shippers (
        ShipperID INT PRIMARY KEY,
        CompanyName VARCHAR(255),
        Phone VARCHAR(24)
    );
    """,
    """
    CREATE TABLE Customers (
        CustomerID INT PRIMARY KEY,
        CompanyName VARCHAR(255),
        ContactName VARCHAR(30),
        ContactTitle VARCHAR(30),
        Address VARCHAR(255),
        City VARCHAR(50),
        Region VARCHAR(50),
        PostalCode VARCHAR(20),
        Country VARCHAR(50),
        Phone VARCHAR(24),
        Fax VARCHAR(24)
    );
    """,
    """
    CREATE TABLE Suppliers (
        SupplierID INT PRIMARY KEY,
        CompanyName VARCHAR(255),
        ContactName VARCHAR(30),
        ContactTitle VARCHAR(30),
        Address VARCHAR(255),
        City VARCHAR(50),
        Region VARCHAR(50),
        PostalCode VARCHAR(20),
        Country VARCHAR(50),
        Phone VARCHAR(24),
        Fax VARCHAR(24),
        Homepage TEXT
    );
    """,
    """
    CREATE TABLE Categories (
        CategoryID INT PRIMARY KEY,
        CategoryName VARCHAR(255),
        Description TEXT,
        Picture IMAGE
    );
    """,
    """
    CREATE TABLE Products (
        ProductID INT PRIMARY KEY,
        ProductName VARCHAR(255),
        SupplierID INT,
        CategoryID INT,
        QuantityPerUnit VARCHAR(255),
        UnitPrice DECIMAL(10, 2),
        UnitsInStock SMALLINT,
        UnitsOnOrder SMALLINT,
        ReorderLevel SMALLINT,
        Discontinued BIT,
        FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID),
        FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
    );
    """,
    """
    CREATE TABLE Orders (
        OrderID INT PRIMARY KEY,
        CustomerID INT,
        EmployeeID INT,
        OrderDate DATE,
        RequiredDate DATE,
        ShippedDate DATE,
        ShipVia INT,
        Freight DECIMAL(10, 2),
        ShipName VARCHAR(255),
        ShipAddress VARCHAR(255),
        ShipCity VARCHAR(50),
        ShipRegion VARCHAR(50),
        ShipPostalCode VARCHAR(20),
        ShipCountry VARCHAR(50),
        FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
        FOREIGN KEY (ShipVia) REFERENCES Shippers(ShipperID)
    );
    """,
    """
    CREATE TABLE OrderDetails (
        OrderID INT,
        ProductID INT,
        UnitPrice DECIMAL(10, 2),
        Quantity SMALLINT,
        Discount DECIMAL(5, 2),
        PRIMARY KEY (OrderID, ProductID),
        FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
        FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
    );
    """,
    """
    CREATE TABLE Employees (
        EmployeeID INT PRIMARY KEY,
        LastName VARCHAR(255),
        FirstName VARCHAR(255),
        Title VARCHAR(50),
        TitleOfCourtesy VARCHAR(25),
        BirthDate DATE,
        HireDate DATE,
        Address VARCHAR(255),
        City VARCHAR(50),
        Region VARCHAR(50),
        PostalCode VARCHAR(20),
        Country VARCHAR(50),
        HomePhone VARCHAR(24),
        Extension VARCHAR(4),
        Photo IMAGE,
        Notes TEXT,
        ReportsTo INT,
        PhotoPath VARCHAR(255),
        FOREIGN KEY (ReportsTo) REFERENCES Employees(EmployeeID)
    );
    """,
    """
    CREATE TABLE Regions (
        RegionID INT PRIMARY KEY,
        RegionDescription VARCHAR(50)
    );
    """,
    """
    CREATE TABLE Territories (
        TerritoryID INT PRIMARY KEY,
        TerritoryDescription VARCHAR(50),
        RegionID INT,
        FOREIGN KEY (RegionID) REFERENCES Regions(RegionID)
    );
    """,
    """
    CREATE TABLE EmployeeTerritories (
        EmployeeID INT,
        TerritoryID INT,
        PRIMARY KEY (EmployeeID, TerritoryID),
        FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
        FOREIGN KEY (TerritoryID) REFERENCES Territories(TerritoryID)
    );
    """
]

# Execute each query in loop 
n=1
for query in queries:
    try:
        cursor.execute(query)
        print("Table created successfully.")
    except Exception as e:
        print(f"An error occurred in creating table {n} error is : {e}")
    n+=1    

conn.commit()

cursor.close()
conn.close()
