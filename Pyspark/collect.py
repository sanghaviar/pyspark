from pyspark.sql import SparkSession
spark =SparkSession.builder.getOrCreate()
data = [(1,'abc','1ssb2'),(2,'abd','1ssb3'),(3,'abe','1ssb4')]
schema = ['slno','name','id']
df = spark.createDataFrame(data,schema)
df2=df.collect()
print(df2[1][1])

