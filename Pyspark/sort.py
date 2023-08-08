from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data = [(1,'maher','m',2000,'IT'),
        (2,'john','m',3000,'HR'),
        (3,'liz','f',4000,'payroll'),
        (4,'saraf','m',5000,'HR')]
schema = ['id','name','gender','salary','dep']
df = spark.createDataFrame(data,schema)
# df.show()
# sort data according to dept column
# df.sort('dep').show() #or
# df.sort(df.dep,df.id.desc()).show()
df.sort(df.dep.desc(),df.id.desc()).show()