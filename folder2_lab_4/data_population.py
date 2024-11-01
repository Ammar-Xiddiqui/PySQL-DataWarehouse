import pyodbc
from faker import Faker
import random

fake = Faker()

server_name = 'DESKTOP-G4025QF\\AMMAR'  # Replace with your server name
datawarehouse_name = 'northwind'  

num_records = 50

try:
    with pyodbc.connect(
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={server_name};Database={datawarehouse_name};Trusted_Connection=yes;",
        autocommit=True
    ) as conn:
        with conn.cursor() as cursor:

            # Populate Category Table
            for _ in range(5):
                cursor.execute("INSERT INTO Category (CategoryKey, CategoryName, Description) VALUES (?, ?, ?)",
                               (_, fake.word(), fake.text()))

            # Populate Continent Table
            continents = ['Asia', 'Africa', 'North America', 'South America', 'Europe', 'Australia']
            for index, continent in enumerate(continents, start=1):
                cursor.execute("INSERT INTO Continent (ContinentKey, ContinentName) VALUES (?, ?)",
                               index, continent)

            # Populate Country Table
            for i in range(num_records):
                cursor.execute("INSERT INTO Country (CountryKey, CountryName, CountryCode, CountryCapital, Population, Subdivision, ContinentKey) VALUES (?, ?, ?, ?, ?, ?, ?)",
                               i, fake.country(), fake.country_code(), fake.city(), fake.random_int(min=100000, max=100000000),
                               fake.state(), random.randint(1, len(continents)))  #

            # Populate State Table
            for i in range(num_records):
                cursor.execute("INSERT INTO State (StateKey, StateName, EnglishStateName, StateType, StateCode, StateCapital, RegionName, RegionCode, CountryKey) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               i, fake.state(), fake.state(), 'State', fake.state_abbr(), fake.city(),
                               fake.region(), fake.state_abbr(), random.randint(1, num_records))  

            # Populate City Table
            for i in range(num_records):
                cursor.execute("INSERT INTO City (CityKey, CityName, StateKey, CountryKey) VALUES (?, ?, ?, ?)",
                               i, fake.city(), random.randint(0, num_records), random.randint(0, num_records))  

            # Populate Supplier Table
            for i in range(num_records):
                cursor.execute("INSERT INTO Supplier (SupplierKey, CompanyName, Address, PostalCode, CityKey) VALUES (?, ?, ?, ?, ?)",
                               i, fake.company(), fake.address(), fake.zipcode(), random.randint(0, num_records)) 

            # Populate Customer Table
            for i in range(num_records):
                cursor.execute("INSERT INTO Customer (CustomerKey, CustomerID, CompanyName, Address, PostalCode, CityKey) VALUES (?, ?, ?, ?, ?, ?)",
                               i, fake.unique.random_int(min=10000, max=99999), fake.company(), fake.address(),
                               fake.zipcode(), random.randint(0, num_records))  

            # Populate Shipper Table
            for i in range(5):
                cursor.execute("INSERT INTO Shipper (ShipperKey, CompanyName) VALUES (?, ?)",
                               i, fake.company())

            # Populate Employee Table
            for i in range(num_records):
                cursor.execute("INSERT INTO Employee (EmployeeKey, FirstName, LastName, Title, BirthDate, HireDate, Address, City, Region, PostalCode, Country, SupervisorKey) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               i, fake.first_name(), fake.last_name(), fake.job(), fake.date_of_birth(minimum_age=18, maximum_age=65),
                               fake.date_this_decade(), fake.address(), fake.city(), fake.state(), fake.zipcode(),
                               fake.country(), random.randint(0, num_records))  

            # Populate Territories Table
            for i in range(num_records):
                cursor.execute("INSERT INTO Territories (EmployeeKey, CityKey) VALUES (?, ?)",
                               random.randint(0, num_records), random.randint(0, num_records)) 

            # Populate Time Table
            for i in range(100):
                cursor.execute("INSERT INTO Time (TimeKey, Date, DayNbWeek, DayNameWeek, DayNbMonth, DayNbYear, WeekNbYear, MonthNumber, MonthName, Quarter, Semester, Year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               i, fake.date_between(start_date='-10y', end_date='today'), fake.day_of_week(),
                               fake.day_name(), fake.day(), fake.year(), fake.week_of_year(), fake.month_number(),
                               fake.month_name(), random.randint(1, 4), random.randint(1, 2), fake.year())

            # Populate Sales Table
            for i in range(num_records):
                cursor.execute("INSERT INTO Sales (OrderNo, CustomerKey, EmployeeKey, OrderDateKey, DueDateKey, ShippedDateKey, ShipperKey, ProductKey, SupplierKey, OrderLineNo, UnitPrice, Quantity, Discount, Freight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               i, random.randint(0, num_records), random.randint(0, num_records),
                               random.randint(0, 99), random.randint(0, 99), random.randint(0, 99), random.randint(0, 4),
                               random.randint(0, num_records), random.randint(0, num_records), 1, fake.pydecimal(left_digits=3, right_digits=2, positive=True),
                               fake.random_int(min=1, max=10), fake.pydecimal(left_digits=2, right_digits=2, positive=True),
                               fake.pydecimal(left_digits=4, right_digits=2, positive=True))

            print("populated successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
