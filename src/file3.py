from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, DateType, StructType
import os


os.environ['PYSPARK_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'


#create empty dataframe with schema...

#there are three ways to create a empty dataframe

Sparkobj = SparkSession.builder.appName("dataframe").getOrCreate()

schema = StructType ([

    StructField("id",IntegerType(),True),
    StructField("emp_name",StringType(),True),
    StructField("date",DateType(),True),

]
)

df = Sparkobj.createDataFrame([],schema)

print(df.show())


#create dataframe from the rdd

rdd  = Sparkobj.sparkContext.emptyRDD()

df1 = Sparkobj.createDataFrame(rdd,schema)

print(df1.show())
