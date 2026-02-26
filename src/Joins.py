# ============================================================
# PySpark Joins Examples
# Concepts: inner join, cross join, outer join, left join,
#           right join, left semi join, left anti join
# ============================================================
from OS.initialize import doinit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

doinit()

# Initialize Spark
spark = SparkSession.builder \
    .appName("PySpark Joins Examples") \
    .getOrCreate()

# Sample DataFrames
data_customers = [
    (1, "Alice", "NY"),
    (2, "Bob", "CA"),
    (3, "Charlie", "TX"),
    (4, "David", "FL")
]

data_orders = [
    (101, 1, "Laptop"),
    (102, 2, "Tablet"),
    (103, 2, "Smartphone"),
    (104, 5, "Monitor")
]

# Create DataFrames
df_customers = spark.createDataFrame(data_customers, ["customer_id", "name", "state"])
df_orders = spark.createDataFrame(data_orders, ["order_id", "customer_id", "product"])

# Show original DataFrames
print("=== Customers ===")
df_customers.show()
print("=== Orders ===")
df_orders.show()

# ============================================================
# 18.1 INNER JOIN
# Only matching rows from both DataFrames
# ============================================================
df_inner = df_customers.join(df_orders, on="customer_id", how="inner")
print("=== Inner Join ===")
df_inner.show()

# ============================================================
# CROSS JOIN
# Cartesian product of both DataFrames
# ============================================================
df_cross = df_customers.crossJoin(df_orders)
print("=== Cross Join ===")
df_cross.show()

# ============================================================
# OUTER JOIN / FULL OUTER JOIN
# All rows from both DataFrames; non-matching values are NULL
# ============================================================
df_outer = df_customers.join(df_orders, on="customer_id", how="outer")
print("=== Full Outer Join ===")
df_outer.show()

# ============================================================
# LEFT JOIN / LEFT OUTER JOIN
# All rows from left DataFrame, matching from right, NULL if no match
# ============================================================
df_left = df_customers.join(df_orders, on="customer_id", how="left")
print("=== Left Join ===")
df_left.show()

# ============================================================
# RIGHT JOIN / RIGHT OUTER JOIN
# All rows from right DataFrame, matching from left, NULL if no match
# ============================================================
df_right = df_customers.join(df_orders, on="customer_id", how="right")
print("=== Right Join ===")
df_right.show()

# ============================================================
# LEFT SEMI JOIN
# Returns only rows from left DataFrame where a match exists in right
# ============================================================
df_left_semi = df_customers.join(df_orders, on="customer_id", how="left_semi")
print("=== Left Semi Join ===")
df_left_semi.show()

# ============================================================
# LEFT ANTI JOIN
# Returns only rows from left DataFrame where no match exists in right
# ============================================================
df_left_anti = df_customers.join(df_orders, on="customer_id", how="left_anti")
print("=== Left Anti Join ===")
df_left_anti.show()

# Stop Spark session
spark.stop()