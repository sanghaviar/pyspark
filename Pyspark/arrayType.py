from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,explode
spark = SparkSession.builder.getOrCreate()
data = [(1,['one','two']),(2,['one','two']),(3,['one','two'])]
schem= StructType([StructField('sl_no',IntegerType()),
                  StructField('Things',ArrayType(StringType()))])
df = spark.createDataFrame(data,schem)
df2 = df.withColumn('Things',explode(col('Things')))
df2.show()