from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
spark = SparkSession.builder.getOrCreate()
data =[(9,'abc','M',20000),(8,'acb','F',30000),(1,'adb',' ',35000)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.sort(df.Name.asc())
df2.show()