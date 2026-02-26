# ============================================================
# PYSPARK ARRAY FUNCTIONS – COMPLETE PRACTICE FILE
# Topic 22: Array Functions
#   22.1 ARRAY(), ARRAY_LENGTH(), ARRAY_POSITION()
#   22.2 ARRAY_CONTAINS(), ARRAY_REMOVE()
# ============================================================
from OS.initialize import doinit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

doinit()

# ------------------------------------------------------------
# 1️⃣ Create Spark Session
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("ArrayFunctionsMaster") \
    .getOrCreate()

# ------------------------------------------------------------
# 2️⃣ Sample Data
# ------------------------------------------------------------
data = [
    (1, ["apple", "banana", "mango"]),
    (2, ["orange", "apple"]),
    (3, ["grapes", "kiwi"]),
    (4, []),
    (5, None)
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("fruits", ArrayType(StringType()), True)
])

df = spark.createDataFrame(data, schema)

print("===== ORIGINAL DATA =====")
df.show(truncate=False)


# ============================================================
# 22.1 ARRAY(), ARRAY_LENGTH(), ARRAY_POSITION()
# ============================================================

print("===== ARRAY() FUNCTION =====")
# Creating a new array column manually
df_array = df.withColumn(
    "new_array",
    array(lit("A"), lit("B"), lit("C"))
)
df_array.show(truncate=False)


print("===== ARRAY_LENGTH() FUNCTION =====")
# Returns number of elements in array
df_length = df.withColumn(
    "array_length",
    size(col("fruits"))   # size() is equivalent to array_length
)
df_length.show(truncate=False)


print("===== ARRAY_POSITION() FUNCTION =====")
# Returns position of element (1-based index)
df_position = df.withColumn(
    "position_of_apple",
    array_position(col("fruits"), "apple")
)
df_position.show(truncate=False)


# ============================================================
# 22.2 ARRAY_CONTAINS(), ARRAY_REMOVE()
# ============================================================

print("===== ARRAY_CONTAINS() FUNCTION =====")
# Check if array contains a specific value
df_contains = df.withColumn(
    "contains_apple",
    array_contains(col("fruits"), "apple")
)
df_contains.show(truncate=False)


print("===== ARRAY_REMOVE() FUNCTION =====")
# Remove a specific element from array
df_removed = df.withColumn(
    "removed_apple",
    array_remove(col("fruits"), "apple")
)
df_removed.show(truncate=False)


# ============================================================
# EXTRA PRACTICE – Combined Example
# ============================================================

print("===== COMBINED OPERATIONS =====")
df_combined = df \
    .withColumn("length", size(col("fruits"))) \
    .withColumn("contains_mango", array_contains(col("fruits"), "mango")) \
    .withColumn("position_mango", array_position(col("fruits"), "mango")) \
    .withColumn("remove_mango", array_remove(col("fruits"), "mango"))

df_combined.show(truncate=False)


# ------------------------------------------------------------
# Stop Spark Session
# ------------------------------------------------------------
spark.stop()

# ============================================================
# END OF FILE
# ============================================================