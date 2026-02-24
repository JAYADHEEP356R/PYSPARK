# ============================================================
# PYSPARK STRING FUNCTIONS – COMPLETE MASTER FILE
# Categorized by Concepts
# Author: Jayadheep R
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("PySpark String Functions Master") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 2. SAMPLE DATA
# ============================================================

data = [
    (1, "  JOHN DOE  ", "John.Doe@gmail.com", "987-654-3210", "apple,banana,mango"),
    (2, "alice smith", "alice@yahoo.com", "9123456780", "cat,dog"),
    (3, None, "invalidemail.com", "99999-88888", "red,blue"),
]

schema = ["id", "name", "email", "phone", "items"]

df = spark.createDataFrame(data, schema)

print("\n========== ORIGINAL DATA ==========")
df.show(truncate=False)


# ============================================================
# 3. CASE CONVERSION FUNCTIONS
# ============================================================

print("\n========== CASE FUNCTIONS ==========")

df_case = df.select(
    col("name"),
    upper("name").alias("upper_name"),
    lower("name").alias("lower_name"),
    initcap("name").alias("initcap_name")
)

df_case.show(truncate=False)


# ============================================================
# 4. TRIM & LENGTH FUNCTIONS
# ============================================================

print("\n========== TRIM & LENGTH FUNCTIONS ==========")

df_trim = df.select(
    col("name"),
    trim("name").alias("trimmed"),
    ltrim("name").alias("left_trim"),
    rtrim("name").alias("right_trim"),
    length("name").alias("name_length")
)

df_trim.show(truncate=False)


# ============================================================
# 5. SUBSTRING FUNCTIONS
# ============================================================

print("\n========== SUBSTRING FUNCTIONS ==========")

df_substring = df.select(
    col("email"),
    substring("email", 1, 5).alias("first_5_chars"),
    substring_index("email", "@", 1).alias("username"),
    substring_index("email", "@", -1).alias("domain")
)

df_substring.show(truncate=False)


# ============================================================
# 6. SEARCH & POSITION FUNCTIONS
# ============================================================

print("\n========== SEARCH FUNCTIONS ==========")

df_instr = df.select(
    col("email"),
    instr("email", "@").alias("position_of_at")
)

df_instr.show()


# ============================================================
# 7. REPLACE FUNCTIONS
# ============================================================

print("\n========== REPLACE FUNCTIONS ==========")

df_replace = df.select(
    col("phone"),
    regexp_replace("phone", "[^0-9]", "").alias("digits_only"),
    translate("name", "aeiouAEIOU", "1234512345").alias("translated_name")
)

df_replace.show(truncate=False)


# ============================================================
# 8. PATTERN MATCHING
# ============================================================

print("\n========== PATTERN MATCHING ==========")

df_pattern = df.select(
    col("email"),
    col("email").like("%gmail%").alias("is_gmail"),
    col("email").rlike("^[A-Za-z0-9+_.-]+@(.+)$").alias("valid_email_format")
)

df_pattern.show(truncate=False)


# ============================================================
# 9. CONCATENATION FUNCTIONS
# ============================================================

print("\n========== CONCAT FUNCTIONS ==========")

df_concat = df.select(
    col("name"),
    concat(lit("User: "), trim("name")).alias("concat_example"),
    concat_ws("-", col("id"), trim("name")).alias("concat_ws_example")
)

df_concat.show(truncate=False)


# ============================================================
# 10. SPLIT & EXPLODE FUNCTIONS
# ============================================================

print("\n========== SPLIT & EXPLODE ==========")

df_split = df.select(
    col("items"),
    split("items", ",").alias("items_array")
)

df_split.show(truncate=False)

print("\n---- Exploded Rows ----")

df_explode = df.select(
    col("id"),
    explode(split("items", ",")).alias("single_item")
)

df_explode.show()


# ============================================================
# 11. PADDING FUNCTIONS
# ============================================================

print("\n========== PADDING FUNCTIONS ==========")

df_pad = df.select(
    col("id"),
    lpad(col("id").cast("string"), 5, "0").alias("left_padded"),
    rpad(col("id").cast("string"), 5, "X").alias("right_padded")
)

df_pad.show()


# ============================================================
# 12. FORMAT & ENCODING FUNCTIONS
# ============================================================

print("\n========== FORMAT & ENCODING ==========")

df_format = df.select(
    format_string("User %s has id %d", trim("name"), col("id")).alias("formatted_string"),
    base64("email").alias("encoded_email"),
    unbase64(base64("email")).alias("decoded_email")
)

df_format.show(truncate=False)


# ============================================================
# 13. NULL HANDLING WITH STRING FUNCTIONS
# ============================================================

print("\n========== NULL HANDLING ==========")

df_null = df.select(
    col("name"),
    coalesce("name", lit("Unknown")).alias("coalesce_example"),
    when(col("name").isNull(), "Missing").otherwise(col("name")).alias("when_example")
)

df_null.show(truncate=False)


# ============================================================
# 14. ADVANCED REAL-WORLD TRANSFORMATIONS
# ============================================================

print("\n========== REAL-WORLD TRANSFORMATIONS ==========")

df_real = df.select(
    col("email"),
    substring_index("email", "@", 1).alias("username"),
    substring_index(substring_index("email", "@", -1), ".", 1).alias("domain_name"),
    substring_index("email", ".", -1).alias("top_level_domain"),
    regexp_replace("phone", "[^0-9]", "").alias("clean_phone")
)

df_real.show(truncate=False)


# ============================================================
# END OF FILE
# ============================================================

spark.stop()