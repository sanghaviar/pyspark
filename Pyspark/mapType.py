from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()

data = [('maheer',{'hair':'black','eye':'brown'}),('John',{'hair':'black','eye':'blue'})]
schm = StructType([StructField('name',StringType()),
                  StructField('properties',MapType(StringType(),StringType()))])
df = spark.createDataFrame(data,schm)
df2 = df.withColumn("keys",df.properties['hair']).show()
df.show()