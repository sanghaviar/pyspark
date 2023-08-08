from pyspark.sql import SparkSession
from pyspark.sql.functions import when
spark = SparkSession.builder.getOrCreate()
data =[(1,'john','M',40000),(2,'anu','F',80000),(3,'mona',' ',68000)]
schema = ['slno','Name','Gender','Salary']
df = spark.createDataFrame(data,schema)
df2 = df.select(df.slno,df.Name,when(df.Gender=='M','male'),when(df.Gender=='F','female').otherwise('unknown'))
df2.show()