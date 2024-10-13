import pyodbc
from faker import Faker
import random

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-G4025QF\\AMMAR;"  # Replace server name
    "Database=northwind;"               
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()
# ----------------------------by faker--------------
fake = Faker()
# populate data in Regions table
def populate_regions(n):
    for _ in range(n):
        region_id = random.randint(1, 1000)  # random region_id
        region_desc = fake.city_suffix()     # Fake city 
        cursor.execute(
            "INSERT INTO Regions (RegionID, RegionDescription) VALUES (?, ?)",
            region_id, region_desc
        )

# populate data in Territories
def populate_territories(n):
    cursor.execute("SELECT RegionID FROM Regions")  # available RegionIDs
    region_ids = [row[0] for row in cursor.fetchall()]

    for _ in range(n):
        territory_id = fake.unique.zipcode()          # Fake and unique territory id
        territory_desc = fake.city()                 # Fake city 
        region_id = random.choice(region_ids)        # Randomly pick a RegionID
        cursor.execute(
            "INSERT INTO Territories (TerritoryID, TerritoryDescription, RegionID) VALUES (?, ?, ?)",
            territory_id, territory_desc, region_id
        )

populate_regions(10)  
populate_territories(20)  
conn.commit()
cursor.close()
conn.close()

