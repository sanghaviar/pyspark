from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[6]").appName("Pyspark").getOrCreate()
data = [(1,"Mher","2000"),
        (2,"Jank","3000")]
columns = ['id','name','salary']
df = spark.createDataFrame(data=data,schema=columns)
# df.show()
df1 = df.withColumnRenamed("salary","salary_amount")
df1.show()