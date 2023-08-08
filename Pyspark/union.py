from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data1 = [(1,'maher','male'),
        (2,'john','male')]
schema1 = ['id','name','gender']
data2 =[(3,'liz','female'),
        (4,'sara','female'),
        (2,'john','male')]
schema2 = ['id','name','gender']

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)
# df1.show()
# df2.show()
# Merged and created new DataFrame and wont delete duplicate rows
newDf = df1.union(df2)
# newDf.show()
newDf1 = df1.unionAll(df2)
# newDf1..show()
# newDf1.distinct().show()
newDf1.distinct().sort(newDf.id.desc()).show()