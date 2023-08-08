from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data1 = [(1,'maher',31)]
schema1 = ['id','name','age']

data2 = [(1,'maher',2000)]
schema2 = ['id','name','salary']

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)
# df1.show()
# df2.show()
# Here it takes only the schema from 1st dataframe
# df1.union(df2).show()
# df1.unionByName(df2).show() # we get error here coz the schema slightly varies
df1.unionByName(df2,allowMissingColumns=True).show() # now we can see all columns