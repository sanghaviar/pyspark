from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
spark = SparkSession.builder.getOrCreate()
data =[(2,'abc','M',50000),(3,'xyz','F',40000),(1,'mno',' ',35000)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.where((df.Gender == 'M') | (df.Gender == 'F'))
df2.show()