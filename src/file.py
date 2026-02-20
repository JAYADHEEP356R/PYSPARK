from pyspark.sql import SparkSession
import os


os.environ['PYSPARK_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'

Spark = SparkSession.builder.master("local[4]").appName("practice").getOrCreate()

print(Spark)

sc = Spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5],2)

rdd2 = rdd.map(lambda x: x*x)

print(rdd2.collect())

print(sc.uiWebUrl)

input("press ENTER to stop the program")