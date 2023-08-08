import pyspark
from pyspark.sql import SparkSession
sc = SparkSession.builder.appName("Spark examples").getOrCreate()
rdd = sc.sparkContext.parallelize([1,2,3,4,5])
# print(rdd.collect())
rddCollect = rdd.collect()
print("Number of partitions: "+str(rdd.getNumPartitions()))
print("Action: 1st element: "+str(rdd.first()))
print(rddCollect)