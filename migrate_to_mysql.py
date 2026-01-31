import pandas as pd
import mysql.connector

# Load Excel/CSV data
df = pd.read_csv("sales_data.csv")

# Remove duplicates
df = df.drop_duplicates()

# Remove rows with missing OrderID
df = df.dropna(subset=["OrderID"])

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="pass@123",
    database="sales_db"
)

cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS sales (
    OrderID INT PRIMARY KEY,
    Customer VARCHAR(50),
    Product VARCHAR(50),
    Amount INT,
    OrderDate DATE
)
""")

# Insert data
for _, row in df.iterrows():
    cursor.execute("""
    INSERT IGNORE INTO sales
    (OrderID, Customer, Product, Amount, OrderDate)
    VALUES (%s, %s, %s, %s, %s)
    """,
    (int(row.OrderID), row.Customer, row.Product,
     int(row.Amount), row.Date))

conn.commit()

print("Migration completed successfully")

conn.close()