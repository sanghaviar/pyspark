from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark = SparkSession.builder.getOrCreate()
data = [(1, "Maher","3000"),
        (2,"Tej","5000"),]
columns = ["id","name","salary"]
df = spark.createDataFrame(data=data,schema=columns)
df.show()
df.printSchema()
# to convert salary to integer
df1 = df.withColumn(colName="salary",col=col("salary").cast("Integer"))
df1.show()
df1.printSchema()

# To update Salary column
df3 = df1.withColumn("salary", col('salary') * 2)
df3.show()

# To add new column to a dataframe
df4 = df1.withColumn("country",lit('India'))
df4.show()