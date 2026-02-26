# ============================================================
# PYSPARK NUMERIC FUNCTIONS – COMPLETE MASTER FILE
# Categorized by Concepts
# Author: Jayadheep R
# ============================================================

from OS.initialize import doinit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

doinit()

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

doinit()

spark = SparkSession.builder \
    .appName("PySpark Numeric Functions Master") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 2. SAMPLE DATA
# ============================================================

data = [
    (1, 10, 5000.75),
    (2, -20, 12000.40),
    (3, 30, 7500.60),
    (4, -40, 15000.00),
    (5, 50, None),
]

schema = ["id", "quantity", "salary"]

df = spark.createDataFrame(data, schema)

print("\n========== ORIGINAL DATA ==========")
df.show()

# ============================================================
# 3. BASIC AGGREGATE FUNCTIONS
# ============================================================

print("\n========== BASIC AGGREGATES ==========")

df_agg = df.select(
    sum("quantity").alias("total_quantity"),
    avg("quantity").alias("average_quantity"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    count("quantity").alias("count_quantity")
)

df_agg.show()

# ============================================================
# 4. MATHEMATICAL FUNCTIONS
# ============================================================

print("\n========== MATHEMATICAL FUNCTIONS ==========")

df_math = df.select(
    col("quantity"),
    abs("quantity").alias("absolute_value"),
    sqrt(abs("quantity")).alias("square_root"),
    pow("quantity", 2).alias("power_2"),
    round("salary", 1).alias("rounded_salary"),
    bround("salary", 1).alias("bankers_round_salary")
)

df_math.show()

# ============================================================
# 5. CEIL, FLOOR & SIGN FUNCTIONS
# ============================================================

print("\n========== CEIL, FLOOR & SIGN ==========")

df_cf = df.select(
    col("salary"),
    ceil("salary").alias("ceil_salary"),
    floor("salary").alias("floor_salary"),
    signum("quantity").alias("sign_of_quantity")
)

df_cf.show()

# ============================================================
# 6. MODULUS & DIVISION FUNCTIONS
# ============================================================

print("\n========== MODULUS & DIVISION ==========")

df_mod = df.select(
    col("quantity"),
    (col("quantity") % 3).alias("modulus_3"),
    expr("quantity div 3").alias("integer_division")
)

df_mod.show()

# ============================================================
# 7. LOGARITHMIC FUNCTIONS
# ============================================================

print("\n========== LOG FUNCTIONS ==========")

df_log = df.select(
    col("quantity"),
    log(10.0, abs("quantity") + 1).alias("log_base_10"),
    log1p(abs("quantity")).alias("log1p"),
    exp("id").alias("exponential")
)

df_log.show()

# ============================================================
# 8. TRIGONOMETRIC FUNCTIONS
# ============================================================

print("\n========== TRIGONOMETRIC FUNCTIONS ==========")

df_trig = df.select(
    col("id"),
    sin("id").alias("sin_value"),
    cos("id").alias("cos_value"),
    tan("id").alias("tan_value"),
    asin(lit(0.5)).alias("asin_example"),
    acos(lit(0.5)).alias("acos_example"),
    atan("id").alias("atan_value")
)

df_trig.show()

# ============================================================
# 9. RANDOM & HASH FUNCTIONS
# ============================================================

print("\n========== RANDOM & HASH ==========")

df_rand = df.select(
    col("id"),
    rand().alias("random_value"),
    randn().alias("random_normal"),
    hash("id").alias("hash_value")
)

df_rand.show()

# ============================================================
# 10. STATISTICAL FUNCTIONS
# ============================================================

print("\n========== STATISTICAL FUNCTIONS ==========")

df_stats = df.select(
    stddev("salary").alias("stddev_salary"),
    variance("salary").alias("variance_salary"),
    skewness("quantity").alias("skewness_quantity"),
    kurtosis("quantity").alias("kurtosis_quantity")
)

df_stats.show()

# ============================================================
# 11. APPROX & PERCENTILE FUNCTIONS
# ============================================================

print("\n========== APPROX FUNCTIONS ==========")

df_percentile = df.select(
    expr("percentile_approx(quantity, 0.5)").alias("median_quantity")
)

df_percentile.show()

# ============================================================
# 12. WINDOW + NUMERIC FUNCTIONS (ADVANCED)
# ============================================================

print("\n========== WINDOW FUNCTIONS ==========")

from pyspark.sql.window import Window

window_spec = Window.orderBy("salary")

df_window = df.select(
    col("id"),
    col("salary"),
    rank().over(window_spec).alias("rank_salary"),
    dense_rank().over(window_spec).alias("dense_rank_salary"),
    row_number().over(window_spec).alias("row_number_salary"),
    lag(col="salary").over(window_spec).alias("previous_salary")
)

df_window.show()

# ============================================================
# 13. NULL HANDLING WITH NUMERIC DATA
# ============================================================

print("\n========== NULL HANDLING ==========")

df_null = df.select(
    col("salary"),
    coalesce("salary", lit(0)).alias("salary_default_0"),
    when(col("salary").isNull(), 0).otherwise(col("salary")).alias("when_example")
)

df_null.show()

# ============================================================
# 14. REAL-WORLD TRANSFORMATIONS
# ============================================================

print("\n========== REAL-WORLD TRANSFORMATIONS ==========")

df_real = df.select(
    col("salary"),
    round(col("salary") * 1.10, 2).alias("salary_with_bonus_10%"),
    abs("quantity").alias("absolute_quantity"),
    ceil(col("salary") / 1000).alias("salary_in_thousands_ceiled")
)

df_real.show()

# ============================================================
# END OF FILE
# ============================================================

spark.stop()