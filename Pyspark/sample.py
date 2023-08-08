from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.range(start=1,end=101)
df.show() # by default id is created
# getting 10% of rows
df1 = df.sample(fraction=0.1)
df1 = df.sample(fraction=0.1,seed = 123)
# Seed will help u to get fixed set of rows
df.show()

