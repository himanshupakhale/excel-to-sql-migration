import pandas as pd
import mysql.connector


print("Starting data migration...")


# Step 1: Load file
try:
    data = pd.read_csv("sales_data.csv")
    print("File loaded successfully")
except Exception as e:
    print("Error loading file:", e)
    exit()


print("Total rows in file:", len(data))


# Step 2: Basic cleaning
print("Removing duplicate rows...")
data = data.drop_duplicates()

print("Rows after duplicate removal:", len(data))


print("Removing rows without OrderID...")
data = data.dropna(subset=["OrderID"])

print("Rows after removing empty OrderID:", len(data))


# Step 3: Connect to database
print("Connecting to database...")

try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="pass@123",   # change locally
        database="sales_db"
    )
    cursor = db.cursor()
    print("Database connected")
except Exception as e:
    print("Database connection failed:", e)
    exit()


# Step 4: Create table (if not exists)
print("Checking table...")

create_table_query = """
CREATE TABLE IF NOT EXISTS sales_data (
    order_id INT,
    customer_name VARCHAR(100),
    product_name VARCHAR(100),
    amount INT,
    order_date DATE
)
"""

cursor.execute(create_table_query)
print("Table ready")


# Step 5: Insert records
print("Starting data insert...")

insert_query = """
INSERT INTO sales_data
(order_id, customer_name, product_name, amount, order_date)
VALUES (%s, %s, %s, %s, %s)
"""

success = 0
failed = 0


for i in range(len(data)):

    row = data.iloc[i]

    try:
        order_id = int(row["OrderID"])
        customer = str(row["Customer"])
        product = str(row["Product"])
        amount = int(row["Amount"])
        date = row["Date"]

        values = (order_id, customer, product, amount, date)

        cursor.execute(insert_query, values)

        success += 1

        if success % 2 == 0:
            print("Inserted", success, "rows")

    except Exception as e:
        failed += 1
        print("Row failed:", i, e)


# Step 6: Commit
print("Committing changes...")

db.commit()


# Step 7: Close connection
cursor.close()
db.close()

print("Migration finished")
print("Successful rows:", success)
print("Failed rows:", failed)