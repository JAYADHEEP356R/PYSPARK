# ============================================================
# PYSPARK DATE & TIME FUNCTIONS – COMPLETE MASTER FILE
# Categorized by Concepts
# Author: Jayadheep R
# ============================================================

from OS.initialize import doinit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

doinit()

spark = SparkSession.builder \
    .appName("PySpark Date & Time Functions Master") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 2. SAMPLE DATA
# ============================================================

data = [
    (1, "2024-01-15", "2024-01-15 10:30:00"),
    (2, "2023-06-20", "2023-06-20 18:45:30"),
    (3, "2022-12-31", "2022-12-31 23:59:59"),
]

schema = ["id", "date_str", "timestamp_str"]

df = spark.createDataFrame(data, schema)

print("\n========== ORIGINAL DATA ==========")
df.show(truncate=False)

# ============================================================
# 3. STRING TO DATE / TIMESTAMP
# ============================================================

print("\n========== CASTING FUNCTIONS ==========")

df_cast = df.select(
    col("date_str"),
    to_date("date_str").alias("converted_date"),
    to_timestamp("timestamp_str").alias("converted_timestamp")
)

df_cast.show(truncate=False)

# ============================================================
# 4. CURRENT DATE & TIME
# ============================================================

print("\n========== CURRENT DATE & TIME ==========")

df_current = df.select(
    current_date().alias("current_date"),
    current_timestamp().alias("current_timestamp")
)

df_current.show(truncate=False)

# ============================================================
# 5. DATE EXTRACTION FUNCTIONS
# ============================================================

print("\n========== DATE EXTRACTION ==========")

df_extract = df.select(
    to_date("date_str").alias("date"),
    year("date_str").alias("year"),
    month("date_str").alias("month"),
    dayofmonth("date_str").alias("day"),
    dayofyear("date_str").alias("day_of_year"),
    weekofyear("date_str").alias("week_of_year"),
    quarter("date_str").alias("quarter")
)

df_extract.show()

# ============================================================
# 6. DATE ARITHMETIC
# ============================================================

print("\n========== DATE ARITHMETIC ==========")

df_arith = df.select(
    to_date("date_str").alias("date"),
    date_add("date_str", 10).alias("add_10_days"),
    date_sub("date_str", 5).alias("subtract_5_days"),
    add_months("date_str", 2).alias("add_2_months"),
    datediff(current_date(), "date_str").alias("days_difference")
)

df_arith.show()

# ============================================================
# 7. MONTH & LAST DAY FUNCTIONS
# ============================================================

print("\n========== MONTH FUNCTIONS ==========")

df_month = df.select(
    to_date("date_str").alias("date"),
    last_day("date_str").alias("last_day_of_month"),
    next_day("date_str", "Sunday").alias("next_sunday")
)

df_month.show()

# ============================================================
# 8. DATE FORMATTING
# ============================================================

print("\n========== DATE FORMATTING ==========")

df_format = df.select(
    to_date("date_str").alias("date"),
    date_format("date_str", "dd-MM-yyyy").alias("formatted_date"),
    date_format("timestamp_str", "yyyy/MM/dd HH:mm").alias("formatted_timestamp")
)

df_format.show(truncate=False)

# ============================================================
# 9. TIMESTAMP FUNCTIONS
# ============================================================

print("\n========== TIMESTAMP FUNCTIONS ==========")

df_timestamp = df.select(
    to_timestamp("timestamp_str").alias("timestamp"),
    hour("timestamp_str").alias("hour"),
    minute("timestamp_str").alias("minute"),
    second("timestamp_str").alias("second")
)

df_timestamp.show()

# ============================================================
# 10. UNIX TIME FUNCTIONS
# ============================================================

print("\n========== UNIX TIME FUNCTIONS ==========")

df_unix = df.select(
    unix_timestamp("timestamp_str").alias("unix_time"),
    from_unixtime(unix_timestamp("timestamp_str")).alias("converted_back")
)

df_unix.show(truncate=False)

# ============================================================
# 11. TIMEZONE FUNCTIONS
# ============================================================

print("\n========== TIMEZONE FUNCTIONS ==========")

df_timezone = df.select(
    to_timestamp("timestamp_str").alias("timestamp"),
    from_utc_timestamp(to_timestamp("timestamp_str"), "Asia/Kolkata").alias("IST_time"),
    to_utc_timestamp(to_timestamp("timestamp_str"), "Asia/Kolkata").alias("UTC_time")
)

df_timezone.show(truncate=False)

# ============================================================
# 12. TRUNCATION FUNCTIONS
# ============================================================

print("\n========== TRUNC FUNCTIONS ==========")

df_trunc = df.select(
    to_date("date_str").alias("date"),
    trunc("date_str", "month").alias("trunc_to_month"),
    trunc("date_str", "year").alias("trunc_to_year")
)

df_trunc.show()

# ============================================================
# 13. NULL HANDLING
# ============================================================

print("\n========== NULL HANDLING ==========")

df_null = df.select(
    col("date_str"),
    coalesce("date_str", lit("1900-01-01")).alias("default_date"),
    when(col("date_str").isNull(), current_date()).otherwise("date_str").alias("when_example")
)

df_null.show()

# ============================================================
# 14. REAL-WORLD TRANSFORMATIONS
# ============================================================

print("\n========== REAL-WORLD EXAMPLES ==========")

df_real = df.select(
    col("date_str"),
    when(year("date_str") < 2023, "Old Record")
    .otherwise("Recent Record")
    .alias("record_category"),
    datediff(current_date(), "date_str").alias("days_old"),
    add_months("date_str", 6).alias("expiry_date")
)

df_real.show()

# ============================================================
# END OF FILE
# ============================================================

spark.stop()