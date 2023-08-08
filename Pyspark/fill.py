from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data = [(1,'maher','m',None),
        (2,'john','m','IT'),
        (3,'mohan',None,'HR'),
        (4,'sara','f','HR'),
        (5,'liz','f','PAYROLL'),
        (6,'nancy','f','IT'),
        (7,'ayasah','f','IT')]
schema = ['id','name','gender','dep']
df = spark.createDataFrame(data,schema)
# df.show()
# df.fillna("unknown").show()
# df.fillna("unknown",['gender']).show() # here we are explicitly telling
df.na.fill("unknown",['dep']).show() # here we are explicitly telling


