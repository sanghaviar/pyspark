from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
spark = SparkSession.builder.getOrCreate()
data =[(2,'abc','M',40000),(3,'anu','F',39000),(1,'John',' ',35000)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.filter(df.Gender=='M')
df2.show()