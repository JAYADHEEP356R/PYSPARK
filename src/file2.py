import sys

from pyspark.sql import SparkSession
import os


os.environ['PYSPARK_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'


Spark = SparkSession.builder.master("local[4]").appName("Stages_Task").getOrCreate()

#print(Spark)
#print(Spark.sparkContext)

sc = Spark.sparkContext

rdd =sc.parallelize([(1,2,3,4),(5,6,7,8)],2) # rdd with 2 partitions created

print("number of partitions :",rdd.getNumPartitions())

print("initial data structure",rdd.collect())

rdd1 = rdd.map(lambda x : (x[0],x[1]+1)) \
    .filter(lambda x: (x[1]>3))  #narrow trasnformation


print("narrow trasformation (map and filter) : ",rdd1.collect())

rdd2 = rdd1.reduceByKey(lambda a,b : a+b) #wide transformation

print("wide transformation (reducebykey): ", rdd2.collect())


print(sc.uiWebUrl)
print(sys.executable)

input("press enter to stop")