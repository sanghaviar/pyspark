#Read a csv file in pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# we need to put double slash every where
df = spark.read.csv("C:\\Users\\Sanghavi\\Desktop\\New folder\\8836201-6f9306ad21398ea43cba4f7d537619d0e07d5ae3\\8836201-6f9306ad21398ea43cba4f7d537619d0e07d5ae3\\iris.csv",
                    inferSchema=True, header=True)
# here inferschema and header is set to true because to show the header
df.show()