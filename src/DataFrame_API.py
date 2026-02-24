# ===============================
# FULL PYSPARK DATAFRAME PRACTICE
# ===============================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'

# -------------------------------
# 1. Create Spark Session
# -------------------------------

spark = SparkSession.builder \
    .appName("FullDataFramePractice") \
    .getOrCreate()

# -------------------------------
# 2. Create Schema
# -------------------------------

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# -------------------------------
# 3. Create DataFrame
# -------------------------------

data = [
    (1, "Alice", "HR", 50000),
    (2, "Bob", "IT", 60000),
    (3, "Charlie", "IT", 70000),
    (4, "David", "Finance", 55000),
    (5, "Eva", "HR", 65000)
]

df = spark.createDataFrame(data, schema)

print("Initial Data:")
df.show()

# -------------------------------
# 4. Basic Transformations
# -------------------------------

print("Select Columns:")
df.select("name", "salary").show()

print("Filter Salary > 60000:")
df.filter(col("salary") > 60000).show()


# -------------------------------
# 5. Add New Column
# -------------------------------

df = df.withColumn("bonus", col("salary") * 0.10)

print("After Adding Bonus Column:")
df.show()

# -------------------------------
# 6. Conditional Column (when)
# -------------------------------

df = df.withColumn(
    "category",
    when(col("salary") > 60000, "High")
    .when(col("salary") > 50000, "Medium")
    .otherwise("Low")
)

print("After Adding Category:")
df.show()

# -------------------------------
# 7. GroupBy and Aggregation
# -------------------------------

print("Department Wise Avg Salary:")
df.groupBy("department") \
    .agg(avg("salary").alias("avg_salary")) \
    .show()

# -------------------------------
# 8. Join Example
# -------------------------------

dept_data = [
    ("HR", "Human Resource"),
    ("IT", "Information Tech"),
    ("Finance", "Finance Dept")
]

dept_df = spark.createDataFrame(dept_data, ["department", "dept_full_name"])

joined_df = df.join(dept_df, on="department", how="inner")

print("After Join:")
joined_df.show()

# -------------------------------
# 9. Union Example
# -------------------------------

new_data = [(6, "Frank", "IT", 72000, 7200.0, "High")]
new_df = spark.createDataFrame(new_data, df.columns)

df = df.union(new_df)

print("After Union:")
df.show()

# -------------------------------
# 10. Window Function
# -------------------------------

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

df = df.withColumn("rank_in_dept", row_number().over(window_spec))

print("After Window Function (Ranking):")
df.show()

# -------------------------------
# 11. Handling Nulls
# -------------------------------

df_null = df.withColumn("salary", when(col("id") == 2, None).otherwise(col("salary")))

print("With Null Value:")
df_null.show()

print("Fill Null:")
df_null.fillna({"salary": 0}).show()

# -------------------------------
# 12. Repartition & Coalesce
# -------------------------------

print("Partitions Before:", df.rdd.getNumPartitions())

df_repart = df.repartition(4)
print("After Repartition:", df_repart.rdd.getNumPartitions())

df_coalesce = df_repart.coalesce(2)
print("After Coalesce:", df_coalesce.rdd.getNumPartitions())

# -------------------------------
# 13. Explain Execution Plan
# -------------------------------

print("Execution Plan:")
df.explain()


# Uncomment if needed
# df.write.mode("overwrite").csv("output_csv")
# df.write.mode("overwrite").parquet("output_parquet")

spark.stop()