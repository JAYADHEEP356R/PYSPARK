from OS.initialize import doinit
from pyspark.sql import SparkSession
#from pyspark.sql.connect.functions import  *
from pyspark.sql.functions import *

doinit()

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


#basic case conversion functions


df1 = df.select(

    col("name"),upper("name").alias("upper_case") , lower("name").alias("lower_case") ,initcap("name").alias("firstCap")

).show()


# basic trim functions

df2 = df.select(

    col("name"), trim("name"),ltrim("name"),rtrim("name")

).show()

#basic substring function

df3 = df.select(


    col("name"), substring("email",1,5) , substring_index("email","@",1) , substring_index("email","@",-1)
).show()

#basic search functions

df4 = df.select(

    col("name") , instr("email","@")

).show()

#pattern matching functions

df5  = df.select(


    col("email").like("%gmail%") , col("email").rlike("^[A-Za-z_.0-9]+@(.+)")

).show()


# concat funtions

df6  = df.select(

    col("email") , concat(lit("User :") , col("email"))

).show()

#replace functions


df6 = df.select(


    regexp_replace("email","[^0-9]" ,""),
    #regexp_extract()
    translate("email","123456789","abcdefghi")

).show()

df6 = df.select(

    col("email"), split("email","@")

).show()


#repeat, rpad , lpad and length col("email").like ...there is no replace only regex_replay()









