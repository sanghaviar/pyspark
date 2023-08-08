from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
spark = SparkSession.builder.getOrCreate()
data =[(2,'abc','M',40700),(3,'acb','F',39080),(1,'adb',' ',359900)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.sort(df.slno.desc())
df2.show()