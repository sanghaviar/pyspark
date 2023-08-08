from pyspark.sql import SparkSession
from pyspark.sql.functions import when
spark = SparkSession.builder.getOrCreate()
data =[(1,'abc','M',3000),(2,'xyz','F',40000),(3,'mno',' ',30000)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.select(df.slno.alias('NO'),df.Name.alias('User_name'))
df2.show()