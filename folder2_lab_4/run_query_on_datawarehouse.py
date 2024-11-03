import pyodbc

server_name = 'DESKTOP-G4025QF\\AMMAR'  # Replace with your server name
datawarehouse_name = 'northwind_lab_5'

try:
    with pyodbc.connect(
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={server_name};Database={datawarehouse_name};Trusted_Connection=yes;",
        autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            # Query 1: Monthly Sales by Customer State Compared to Previous Year
            monthly_sales_by_state_query = """
            SELECT 
                YEAR(S.OrderDate) AS Year,
                MONTH(S.OrderDate) AS Month,
                C.State,
                SUM(S.SalesAmount) AS MonthlySales,
                LAG(SUM(S.SalesAmount), 12) OVER (PARTITION BY C.State ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate)) AS PreviousYearSales,
                CASE 
                    WHEN LAG(SUM(S.SalesAmount), 12) OVER (PARTITION BY C.State ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate)) IS NULL THEN NULL
                    ELSE (SUM(S.SalesAmount) - LAG(SUM(S.SalesAmount), 12) OVER (PARTITION BY C.State ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate))) * 100.0 / LAG(SUM(S.SalesAmount), 12) OVER (PARTITION BY C.State ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate))
                END AS GrowthPercentage
            FROM Sales S
            JOIN Customer C ON S.CustomerKey = C.CustomerKey
            GROUP BY YEAR(S.OrderDate), MONTH(S.OrderDate), C.State
            ORDER BY Year, Month, C.State;
            """
            cursor.execute(monthly_sales_by_state_query)
            monthly_sales_by_state = cursor.fetchall()
            print("Monthly Sales by Customer State:")
            for row in monthly_sales_by_state:
                print(row)

            # Query 2: Monthly Sales Growth per Product
            monthly_sales_growth_per_product_query = """
            SELECT 
                YEAR(S.OrderDate) AS Year,
                MONTH(S.OrderDate) AS Month,
                S.ProductKey,
                SUM(S.SalesAmount) AS TotalSales,
                LAG(SUM(S.SalesAmount)) OVER (PARTITION BY S.ProductKey ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate)) AS PreviousMonthSales,
                CASE 
                    WHEN LAG(SUM(S.SalesAmount)) OVER (PARTITION BY S.ProductKey ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate)) IS NULL THEN NULL
                    ELSE (SUM(S.SalesAmount) - LAG(SUM(S.SalesAmount)) OVER (PARTITION BY S.ProductKey ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate))) * 100.0 / LAG(SUM(S.SalesAmount)) OVER (PARTITION BY S.ProductKey ORDER BY YEAR(S.OrderDate), MONTH(S.OrderDate))
                END AS GrowthPercentage
            FROM Sales S
            GROUP BY YEAR(S.OrderDate), MONTH(S.OrderDate), S.ProductKey
            ORDER BY Year, Month, S.ProductKey;
            """
            cursor.execute(monthly_sales_growth_per_product_query)
            monthly_sales_growth_per_product = cursor.fetchall()
            print("\nMonthly Sales Growth per Product:")
            for row in monthly_sales_growth_per_product:
                print(row)

            # Query 3: Top Three Best-Selling Employees
            top_three_employees_query = """
            SELECT 
                S.EmployeeKey,
                SUM(S.SalesAmount) AS TotalSales
            FROM Sales S
            GROUP BY S.EmployeeKey
            ORDER BY TotalSales DESC
            OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY;
            """
            cursor.execute(top_three_employees_query)
            top_three_employees = cursor.fetchall()
            print("\nTop Three Best-Selling Employees:")
            for row in top_three_employees:
                print(row)

except Exception as e:
    print(f"An error occurred: {e}")
