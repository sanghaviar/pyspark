from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark = SparkSession.builder.getOrCreate()
data = [('IT',8,5),
        ('PayRoll',3,2),
        ('HR',2,4)]
schema = ['dep','male','female']
df = spark.createDataFrame(data,schema)
unpivot = df.select('dep',expr("stack(2,'male',male,'female',female) as(gender,count)"))
unpivot.show()
