from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date
import os


os.environ['PYSPARK_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'


Sparkobj = SparkSession.builder.appName("dataframe").master("local[4]").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("date", DateType(), True)
])

# empty df
rdd = Sparkobj.sparkContext.emptyRDD()
df1 = Sparkobj.createDataFrame(rdd, schema)

# correct new row (use datetime.date for DateType)
new_row = Sparkobj.createDataFrame(
    [(1, "jai", date(2004, 12, 20))],
    schema
)

# assign union result
df1 = df1.union(new_row)

df1.show()