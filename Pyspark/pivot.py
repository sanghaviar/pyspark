from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'maher','m','IT'),
        (2,'john','m','IT'),
        (3,'mohan','m','HR'),
        (4,'sara','f','HR'),
        (5,'liz','f','PAYROLL'),
        (6,'nancy','f','IT'),
        (7,'ayasah','f','IT')]
schema = ['id','name','gender','dep']
df = spark.createDataFrame(data,schema)
df = df.groupby('dep','gender').count()
df.groupBy('dep').pivot('gender').count().show()
df.groupBy('dep').pivot('gender',['m']).count().show()

