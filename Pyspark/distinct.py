from pyspark.sql import SparkSession
from pyspark.sql.functions import when
spark = SparkSession.builder.getOrCreate()
data =[(1,'abc','M',20000),(2,'xyz','F',30000),(3,'abc','M',35000),(3,'anu','F',35000)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.select(df.Gender,df.Name).distinct()
df2.show()