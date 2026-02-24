import re

from pyspark.sql import SparkSession

import os

os.environ['PYSPARK_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\raghu\PycharmProjects\Stages_Tasks\.venv\Scripts\python.exe'


Spark = SparkSession.builder.master("local[*]").appName("practice").getOrCreate()

sc = Spark.sparkContext

rdd = sc.textFile("file:///C:/Users/raghu/OneDrive/Desktop/testing.txt",2)

words = rdd.map(lambda line: re.sub(r"[^\w\s]","",line).lower().split()) #applies the lambda function for each element ... /w->all alphabets /s->white spaces ^ not

print(words.getNumPartitions())


result = words.collect()

print(result)


print("=============================================================================================================================")

#on the given numbers ... return only the sum of the even numbers

rdd  = sc.parallelize([1,2,4,3,9,8],2)


rdd1 = rdd.filter(lambda x: x%2==0) \
       .reduce(lambda a,b: a+b)

print("the sum of the even numbers :",rdd1)


print("=============================================================================================================================")

#in the given list remove the duplicates without using distinct()
list1 = [1,2,2,3,4,4,5]
rdd = sc.parallelize([1,2,2,3,4,4,5])

result = (
    rdd.map(lambda x: (x,1))
       .reduceByKey(lambda a,b: a+b)
)

result_og = result.map(lambda x: x[0])


print("the grouped values are",result_og.collect())



