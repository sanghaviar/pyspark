from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data1 = [(1,'maher',2000,2),
        (2,'john',3000,1),
        (3,'sara',4000,4)]
schema1 = ['id','name','salary','dep']

data2 = [(1,'IT'),
         (2,'HR'),
         (3,'Payroll')]
schema2 = ['id','name']

empDf = spark.createDataFrame(data1,schema1)
depDf = spark.createDataFrame(data2,schema2)
empDf.show()
depDf.show()
empDf.join(depDf, empDf.dep == depDf.id,'inner').show()
empDf.join(depDf, empDf.dep == depDf.id,'left').show()
empDf.join(depDf, empDf.dep == depDf.id,'right').show()
empDf.join(depDf, empDf.dep == depDf.id,'full').show()