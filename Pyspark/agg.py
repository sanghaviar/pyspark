from pyspark.sql import SparkSession
from pyspark.sql.functions import count,min,max
spark = SparkSession.builder.getOrCreate()

data = [(1,'maher','male',20000,'IT'),
        (2,'john','male',30000,'HR'),
        (3,'sara','female',40000,'Payroll'),
        (4,'liz','female',20000,'HR'),
        (5,'Nancy','female',80000,'IT'),
        (6,'Saraf','male',20000,'IT')]
schema = ['id','name','gender','salary','dep']
df = spark.createDataFrame(data,schema)
# df.groupby('dep').min('salary').show()
df.groupby('dep').agg(count('*').alias('CountOfEmp'),\
                                       min('salary').alias('minSal'),\
                                       max('salary').alias('maxSal')).show()