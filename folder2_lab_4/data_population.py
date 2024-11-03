import pyodbc
from faker import Faker
import random

fake = Faker()

server_name = 'DESKTOP-G4025QF\\AMMAR'  # Replace with your server name
datawarehouse_name = 'northwind_lab_5'

num_records = 50

try:
    with pyodbc.connect(
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={server_name};Database={datawarehouse_name};Trusted_Connection=yes;",
        autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            # Function to get the maximum key from a table
            def get_max_key(table_name, key_column):
                cursor.execute(f"SELECT MAX({key_column}) FROM {table_name}")
                result = cursor.fetchone()
                return result[0] if result[0] is not None else 0

            # Get max keys for each dimension table
            max_category_key = get_max_key('Category', 'CategoryKey')
            max_product_key = get_max_key('Product', 'ProductKey')
            max_continent_key = get_max_key('Continent', 'ContinentKey')
            max_country_key = get_max_key('Country', 'CountryKey')
            max_state_key = get_max_key('State', 'StateKey')
            max_city_key = get_max_key('City', 'CityKey')
            max_supplier_key = get_max_key('Supplier', 'SupplierKey')
            max_customer_key = get_max_key('Customer', 'CustomerKey')
            max_shipper_key = get_max_key('Shipper', 'ShipperKey')
            max_employee_key = get_max_key('Employee', 'EmployeeKey')
            max_time_key = get_max_key('Time', 'TimeKey')

            # Insert into Category table
            for i in range(num_records):
                cursor.execute("INSERT INTO Category (CategoryKey, CategoryName, Description) VALUES (?, ?, ?)",
                               (max_category_key + 1 + i, fake.word(), fake.text()))

            # Insert into Product table
            product_keys = []
            for i in range(num_records):
                cursor.execute("INSERT INTO Product (ProductKey, ProductName, QuantityPerUnit, UnitPrice, Discontinued, CategoryKey) VALUES (?, ?, ?, ?, ?, ?)",
                               (max_product_key + 1 + i, fake.word(), fake.random_int(min=1, max=10), 
                                fake.pydecimal(left_digits=3, right_digits=2, positive=True),
                                fake.boolean(), random.randint(1, max_category_key + num_records)))
                product_keys.append(max_product_key + 1 + i)

            # Insert into Continent table
            for i in range(num_records):
                cursor.execute("INSERT INTO Continent (ContinentKey, ContinentName) VALUES (?, ?)",
                               (max_continent_key + 1 + i, fake.continent()))

            # Insert into Country table
            country_keys = []
            for i in range(num_records):
                cursor.execute("INSERT INTO Country (CountryKey, CountryName, CountryCode, CountryCapital, Population, Subdivision, ContinentKey) VALUES (?, ?, ?, ?, ?, ?, ?)",
                               (max_country_key + 1 + i, fake.country(), fake.country_code(), fake.city(), 
                                fake.random_int(min=1_000_000, max=1_000_000_000), fake.state(), 
                                random.randint(1, max_continent_key + num_records)))
                country_keys.append(max_country_key + 1 + i)

            # Insert into State table
            state_keys = []
            for i in range(num_records):
                cursor.execute("INSERT INTO State (StateKey, StateName, EnglishStateName, StateType, StateCode, StateCapital, RegionName, RegionCode, CountryKey) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (max_state_key + 1 + i, fake.state(), fake.state(), fake.random_element(elements=('Type1', 'Type2')),
                                fake.state_abbr(), fake.city(), fake.random_element(elements=('Region1', 'Region2')),
                                fake.random_element(elements=('R1', 'R2')), random.randint(1, max_country_key + num_records)))
                state_keys.append(max_state_key + 1 + i)

            # Insert into City table
            for i in range(num_records):
                cursor.execute("INSERT INTO City (CityKey, CityName, StateKey, CountryKey) VALUES (?, ?, ?, ?)",
                               (max_city_key + 1 + i, fake.city(), random.choice(state_keys), random.choice(country_keys)))

            # Insert into Supplier table
            for i in range(num_records):
                cursor.execute("INSERT INTO Supplier (SupplierKey, CompanyName, Address, PostalCode, CityKey) VALUES (?, ?, ?, ?, ?)",
                               (max_supplier_key + 1 + i, fake.company(), fake.address(), fake.zipcode(), random.choice(state_keys)))

            # Insert into Customer table
            for i in range(num_records):
                cursor.execute("INSERT INTO Customer (CustomerKey, CustomerID, CompanyName, Address, PostalCode, CityKey) VALUES (?, ?, ?, ?, ?, ?)",
                               (max_customer_key + 1 + i, fake.unique.random_int(min=1, max=99999), fake.company(),
                                fake.address(), fake.zipcode(), random.choice(state_keys)))

            # Insert into Shipper table
            for i in range(num_records):
                cursor.execute("INSERT INTO Shipper (ShipperKey, CompanyName) VALUES (?, ?)",
                               (max_shipper_key + 1 + i, fake.company()))

            # Insert into Employee table
            for i in range(num_records):
                cursor.execute("INSERT INTO Employee (EmployeeKey, FirstName, LastName, Title, BirthDate, HireDate, Address, City, Region, PostalCode, Country, SupervisorKey) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (max_employee_key + 1 + i, fake.first_name(), fake.last_name(), fake.job(),
                                fake.date_of_birth(minimum_age=18, maximum_age=65), fake.date_this_decade(), 
                                fake.address(), fake.city(), fake.state(), fake.zipcode(), fake.country(),
                                random.choice([None] + [random.randint(1, max_employee_key + num_records)])))

            # Insert into Time table
            for i in range(num_records):
                date = fake.date_between(start_date='-5y', end_date='today')
                cursor.execute("INSERT INTO Time (TimeKey, Date, DayNbWeek, DayNameWeek, DayNbMonth, DayNbYear, WeekNbYear, MonthNumber, MonthName, Quarter, Semester, Year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (max_time_key + 1 + i, date, date.weekday() + 1, date.strftime("%A"), 
                                date.day, date.month, date.isocalendar()[1], date.month, date.strftime("%B"),
                                (date.month - 1) // 3 + 1, (date.month - 1) // 6 + 1, date.year))

            # Insert into Sales table
            for i in range(num_records):
                cursor.execute("INSERT INTO Sales (OrderNo, CustomerKey, EmployeeKey, OrderDateKey, DueDateKey, ShippedDateKey, ShipperKey, ProductKey, SupplierKey, OrderLineNo, UnitPrice, Quantity, Discount, Freight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (i + 1, random.randint(1, max_customer_key), random.randint(1, max_employee_key),
                                random.randint(1, max_time_key), random.randint(1, max_time_key), random.randint(1, max_time_key),
                                random.randint(1, max_shipper_key), random.choice(product_keys), random.randint(1, max_supplier_key),
                                1, fake.pydecimal(left_digits=3, right_digits=2, positive=True),
                                fake.random_int(min=1, max=10), fake.pydecimal(left_digits=2, right_digits=2, positive=True),
                                fake.pydecimal(left_digits=4, right_digits=2, positive=True)))

            print("Data populated successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
