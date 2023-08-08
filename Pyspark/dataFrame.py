# Create a dataframe from RDD

from pyspark.sql import SparkSession
from datetime import datetime,date
# reate spark session, here spark is an object
spark = SparkSession.builder.getOrCreate()
# Creation of RDD
rdd = spark.sparkContext.parallelize([
(1,1.0,"string1",date(2021,1,1),datetime(2021,1,12,0)),
(2, 2.0, "string2", date(2021, 2, 1), datetime(2021, 1, 2,12, 0)),
(3, 3.0, "string3", date(2021, 3, 1), datetime(2021, 1, 3,12, 0)),
 ])
print(rdd.collect())