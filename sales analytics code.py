# 📦 Import Libraries
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# 🗂 Load Dataset
df = pd.read_csv(r"C:\Users\mehal\OneDrive\Desktop\SEM-3\DATA ANALYTICS\train.csv", encoding='latin1', parse_dates=["Order Date", "Ship Date"])

# Clean column names for SQL compatibility (replace spaces and special characters with underscores)
df.columns = df.columns.str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)

# --- 2. Load it into a SQLite database ---
db_name = 'superstore.db'
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Drop table if it already exists to ensure a clean start
cursor.execute("DROP TABLE IF EXISTS orders")

# Load the DataFrame into a SQLite table
# Using if_exists='replace' will drop the table and re-create it
df.to_sql('orders', conn, if_exists='replace', index=False)
print(f"\nDataset loaded into SQLite database '{db_name}' as table 'orders'.")

# --- 3. Running SQL Queries ---
print("\n--- Running SQL Queries ---")

# Query 1: Total Sales by Product Category
query_sales_by_category = """
SELECT Category, SUM(Sales) AS TotalSales
FROM orders
GROUP BY Category
ORDER BY TotalSales DESC;
"""
sales_by_category = pd.read_sql_query(query_sales_by_category, conn)
print("\nTotal Sales by Product Category:")
print(sales_by_category)

# Query 2: Top 10 Customers by Sales
query_top_customers = """
SELECT Customer_Name, SUM(Sales) AS TotalSales
FROM orders
GROUP BY Customer_Name
ORDER BY TotalSales DESC
LIMIT 10;
"""
top_customers = pd.read_sql_query(query_top_customers, conn)
print("\nTop 10 Customers by Sales:")
print(top_customers)

# Query 3: Monthly Sales Trend
query_monthly_sales = """
SELECT
    strftime('%Y-%m', Order_Date) AS SalesMonth,
    SUM(Sales) AS MonthlySales
FROM orders
GROUP BY SalesMonth
ORDER BY SalesMonth;
"""
# Ensure Order_Date is in datetime format before querying if not already
# For sqlite, strftime works on string dates, assuming they are in 'YYYY-MM-DD' or similar format.
# Let's add a check for the Order_Date column type and convert if necessary
if 'Order_Date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['Order_Date']):
    # Convert 'Order_Date' to datetime objects and then to string for consistent SQLite storage
    df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')
    df.dropna(subset=['Order_Date'], inplace=True) # Drop rows where date conversion failed
    df['Order_Date'] = df['Order_Date'].dt.strftime('%Y-%m-%d')
    # Re-load to DB if dates were converted
    df.to_sql('orders', conn, if_exists='replace', index=False)
    print("\n'Order_Date' column converted to datetime and table re-loaded for accurate monthly sales query.")

monthly_sales = pd.read_sql_query(query_monthly_sales, conn)
print("\nMonthly Sales Trend:")
print(monthly_sales.head()) # Print head as it can be long

# --- 4. Performing basic visualizations using matplotlib and seaborn ---
print("\n--- Generating Visualizations ---")

# Visualization 1: Bar Plot of Total Sales by Category
plt.figure(figsize=(10, 6))
# FIX: Assign 'Category' to hue and set legend=False
sns.barplot(x='Category', y='TotalSales', data=sales_by_category, hue='Category', palette='viridis', legend=False)
plt.title('Total Sales by Product Category')
plt.xlabel('Product Category')
plt.ylabel('Total Sales ($)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('total_sales_by_category.png')
print("Saved 'total_sales_by_category.png'")

# Visualization 2: Bar Plot of Top 10 Customers by Sales
plt.figure(figsize=(12, 7))
# FIX: Assign 'Customer_Name' to hue and set legend=False
sns.barplot(x='TotalSales', y='Customer_Name', data=top_customers, hue='Customer_Name', palette='magma', legend=False)
plt.title('Top 10 Customers by Total Sales')
plt.xlabel('Total Sales ($)')
plt.ylabel('Customer Name')
plt.tight_layout()
plt.savefig('top_10_customers_by_sales.png')
print("Saved 'top_10_customers_by_sales.png'")

# Visualization 3: Line Plot of Monthly Sales Trend (no change needed here)
monthly_sales['SalesMonth'] = pd.to_datetime(monthly_sales['SalesMonth'])
plt.figure(figsize=(14, 7))
sns.lineplot(x='SalesMonth', y='MonthlySales', data=monthly_sales, marker='o')
plt.title('Monthly Sales Trend')
plt.xlabel('Month')
plt.ylabel('Total Monthly Sales ($)')
plt.grid(True)
plt.tight_layout()
plt.savefig('monthly_sales_trend.png')
print("Saved 'monthly_sales_trend.png'")


# --- 5. Exporting results if needed ---
print("\n--- Exporting Query Results ---")

# Export 'sales_by_category' to a CSV file
output_csv_path = 'sales_by_category.csv'
sales_by_category.to_csv(output_csv_path, index=False)
print(f"Exported 'Total Sales by Product Category' to '{output_csv_path}'")

# Close the database connection
conn.close()
print("\nDatabase connection closed.")
