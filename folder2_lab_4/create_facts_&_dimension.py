import pyodbc

server_name = 'DESKTOP-G4025QF\\AMMAR'  # Replace with your server name
datawarehouse_name = 'northwind' 

# SQL statements to create dimension and fact tables
create_tables_sql = """
-- Dimension Tables
CREATE TABLE Category (
    CategoryKey INT PRIMARY KEY,
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX)
);

CREATE TABLE Product (
    ProductKey INT PRIMARY KEY,
    ProductName NVARCHAR(255),
    QuantityPerUnit NVARCHAR(50),
    UnitPrice DECIMAL(18, 2),
    Discontinued BIT,
    CategoryKey INT FOREIGN KEY REFERENCES Category(CategoryKey)
);

CREATE TABLE Continent (
    ContinentKey INT PRIMARY KEY,
    ContinentName NVARCHAR(255)
);

CREATE TABLE Country (
    CountryKey INT PRIMARY KEY,
    CountryName NVARCHAR(255),
    CountryCode NVARCHAR(10),
    CountryCapital NVARCHAR(255),
    Population BIGINT,
    Subdivision NVARCHAR(255),
    ContinentKey INT FOREIGN KEY REFERENCES Continent(ContinentKey)
);

CREATE TABLE State (
    StateKey INT PRIMARY KEY,
    StateName NVARCHAR(255),
    EnglishStateName NVARCHAR(255),
    StateType NVARCHAR(50),
    StateCode NVARCHAR(10),
    StateCapital NVARCHAR(255),
    RegionName NVARCHAR(255),
    RegionCode NVARCHAR(10),
    CountryKey INT FOREIGN KEY REFERENCES Country(CountryKey)
);

CREATE TABLE City (
    CityKey INT PRIMARY KEY,
    CityName NVARCHAR(255),
    StateKey INT NULL FOREIGN KEY REFERENCES State(StateKey),
    CountryKey INT NULL FOREIGN KEY REFERENCES Country(CountryKey)
);

CREATE TABLE Supplier (
    SupplierKey INT PRIMARY KEY,
    CompanyName NVARCHAR(255),
    Address NVARCHAR(255),
    PostalCode NVARCHAR(20),
    CityKey INT FOREIGN KEY REFERENCES City(CityKey)
);

CREATE TABLE Customer (
    CustomerKey INT PRIMARY KEY,
    CustomerID NVARCHAR(5) UNIQUE,
    CompanyName NVARCHAR(255),
    Address NVARCHAR(255),
    PostalCode NVARCHAR(20),
    CityKey INT FOREIGN KEY REFERENCES City(CityKey)
);

CREATE TABLE Shipper (
    ShipperKey INT PRIMARY KEY,
    CompanyName NVARCHAR(255)
);

CREATE TABLE Employee (
    EmployeeKey INT PRIMARY KEY,
    FirstName NVARCHAR(255),
    LastName NVARCHAR(255),
    Title NVARCHAR(255),
    BirthDate DATE,
    HireDate DATE,
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(255),
    SupervisorKey INT NULL
);

CREATE TABLE Territories (
    EmployeeKey INT FOREIGN KEY REFERENCES Employee(EmployeeKey),
    CityKey INT FOREIGN KEY REFERENCES City(CityKey)
);

CREATE TABLE Time (
    TimeKey INT PRIMARY KEY,
    Date DATE,
    DayNbWeek INT,
    DayNameWeek NVARCHAR(50),
    DayNbMonth INT,
    DayNbYear INT,
    WeekNbYear INT,
    MonthNumber INT,
    MonthName NVARCHAR(50),
    Quarter INT,
    Semester INT,
    Year INT
);

-- Fact Table
CREATE TABLE Sales (
    OrderNo INT,  -- Add OrderNo column
    CustomerKey INT FOREIGN KEY REFERENCES Customer(CustomerKey),
    EmployeeKey INT FOREIGN KEY REFERENCES Employee(EmployeeKey),
    OrderDateKey INT FOREIGN KEY REFERENCES Time(TimeKey),
    DueDateKey INT FOREIGN KEY REFERENCES Time(TimeKey),
    ShippedDateKey INT FOREIGN KEY REFERENCES Time(TimeKey),
    ShipperKey INT FOREIGN KEY REFERENCES Shipper(ShipperKey),
    ProductKey INT FOREIGN KEY REFERENCES Product(ProductKey),
    SupplierKey INT FOREIGN KEY REFERENCES Supplier(SupplierKey),
    OrderLineNo INT,
    UnitPrice DECIMAL(18, 2),
    Quantity INT,
    Discount DECIMAL(5, 2),
    SalesAmount AS (Quantity * UnitPrice * (1 - Discount)),
    Freight DECIMAL(18, 2),
    PRIMARY KEY (OrderNo, OrderLineNo)
);
"""

try:
    with pyodbc.connect(
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={server_name};Database={datawarehouse_name};Trusted_Connection=yes;",
        autocommit=True  
    ) as conn:
        
        with conn.cursor() as cursor:
            # Execute the SQL commands 
            cursor.execute(create_tables_sql)
            print("Fact table and dimension table successfully created .")

except Exception as e:
    print(f"An error occurred: {e}")
