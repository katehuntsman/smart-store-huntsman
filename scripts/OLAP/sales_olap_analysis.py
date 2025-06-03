# sales_olap_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, sum as _sum, col
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

# Set absolute path to the sales_data.csv file
ABSOLUTE_PATH = "/Users/katehuntsman/Documents/GitHub/smart-sales/data/raw/sales_data.csv"

# Create Spark session
spark = SparkSession.builder \
    .appName("SmartSales OLAP Analysis") \
    .getOrCreate()

# Load data
df = spark.read.csv(ABSOLUTE_PATH, header=True, inferSchema=True)

# Step 1: Preprocessing – Extract Year, Month, Day
df = df.withColumn("Year", year("sale_date")) \
       .withColumn("Month", month("sale_date")) \
       .withColumn("Day", dayofmonth("sale_date"))

# Step 2: Slice – Filter for Year 2025 only
df_2025 = df.filter(df["Year"] == 2025)

# Step 3: Dice – Group by product category and store_id
sales_by_cat_store = df_2025.groupBy("product_category", "store_id") \
                            .agg(_sum("sale_amount").alias("TotalSales")) \
                            .orderBy("product_category", "store_id")

# Step 4: Trend Analysis – Group by category and month
sales_by_cat_month = df_2025.groupBy("product_category", "Month") \
                            .agg(_sum("sale_amount").alias("TotalSales")) \
                            .orderBy("product_category", "Month")

# Step 5: Drilldown Example – Year → Month → Day
drilldown = df_2025.groupBy("Year", "Month", "Day", "product_category") \
                   .agg(_sum("sale_amount").alias("TotalSales")) \
                   .orderBy("Year", "Month", "Day")

# Step 6: Test & Validate – Print sample totals
print("\n🧪 Total sales by category and store (validation sample):")
sales_by_cat_store.show(5)

print("\n🧪 Total sales by category and month (validation sample):")
sales_by_cat_month.show(5)

# Optional total consistency check
raw_total = df_2025.agg(_sum("sale_amount")).collect()[0][0]
cat_month_total = sales_by_cat_month.agg(_sum("TotalSales")).collect()[0][0]
assert abs(raw_total - cat_month_total) < 1e-6, "❌ Mismatch in totals!"

print(f"\n✅ Total sales validated: {raw_total:.2f} matches aggregated total.")

# Step 7: Visualize using Matplotlib and Seaborn
pdf_cat_month = sales_by_cat_month.toPandas()
pdf_cat_store = sales_by_cat_store.toPandas()

# Line chart: Monthly trends by product category
plt.figure(figsize=(10, 6))
sns.lineplot(data=pdf_cat_month, x="Month", y="TotalSales", hue="product_category", marker="o")
plt.title("Monthly Sales Trend by Product Category (2025)")
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.legend(title="Product Category")
plt.grid(True)
plt.tight_layout()
plt.show()

# Heatmap: Sales by category and store
pivot_table = pdf_cat_store.pivot(index="product_category", columns="store_id", values="TotalSales").fillna(0)

plt.figure(figsize=(12, 6))
sns.heatmap(pivot_table, annot=True, fmt=".0f", cmap="YlGnBu", cbar_kws={"label": "Total Sales"})
plt.title("Sales by Product Category and Store ID (2025)")
plt.xlabel("Store ID")
plt.ylabel("Product Category")
plt.tight_layout()
plt.show()