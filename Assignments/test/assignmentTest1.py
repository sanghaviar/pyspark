import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from Assignments.src.Assignment1 import util_assignment1
def test_time_stamp(df):

    timestamp = df.withColumn('NewTime', from_unixtime(col('Issue_Date') / 1000, "yyyy-MM-dd 'T' HH:mm:ssZZZZ"))

    datetype = timestamp.withColumn("Date", split(col('NewTime'), 'T')[0])

    trim1 = datetype.withColumn('Brands', ltrim(col('Brand')))

    tr = trim1.fillna("Empty")
    return tr
class MyTestCase(unittest.TestCase):
    def test_case1(self):
        spark =SparkSession.builder.getOrCreate()
        # Test case input dataframe
        data = [('Washing Machine', "1648770933000", 20000, 'samsung', 'India', '0001'),
                ('Refrigerator', "1648770999000", 35000, 'LG', None, '0002'),
                ('Air Cooler', "1648770948000", 45000, 'Voltas', None, '0003')]
        schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
        df_test = spark.createDataFrame(data, schema)

        # # output dataframe
        # dataoutput1= [('Washing Machine',"1648770933000",20000,'samsung','India','0001','2022-04-01 T 05:25:33GMT+05:30','2022-04-01 ','samsung'),
        #         ('Refrigerator',"1648770999000",35000, 'LG','Empty','0002','2022-04-01 T 05:26:39GMT+05:30','2022-04-01 ','LG'),
        #         ('Air Cooler',"1648770948000",45000,'Voltas','Empty','0003','2022-04-01 T 05:25:48GMT+05:30','2022-04-01 ','Voltas')]
        #
        # # Define the schema for the DataFrame
        # schemaoutput1 = ['Product Name','Issue_Date','Price','Brand','Country','product_number','NewTime','Date','Brands']
        # test1output_df =spark.createDataFrame(dataoutput1,schemaoutput1)
        output_df = util_assignment1.time_stamp(df_test)
        self.assertEqual(output_df.collect(),test_time_stamp(df_test).collect())

    def test_case2(self):
        spark = SparkSession.builder.getOrCreate()
        # Test case input dataframe
        data = [('Mobile', "1948770833000", 40000, 'oppo', 'USA', '0005'),
                ('Television', "1048770899000", 35900, 'tcl', None, '0006'),
                ('watch', "1748770988000", 45800, 'boat', None, '0007')]
        schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
        df_test = spark.createDataFrame(data, schema)
        output_df = util_assignment1.time_stamp(df_test)
        self.assertEqual(output_df.collect(), test_time_stamp(df_test).collect())

    def test_case3(self):
        spark = SparkSession.builder.getOrCreate()
        # Test case input dataframe
        data = [('Laptop', "1888870833999", 900000, 'oppo', 'USA', '0005'),
                ('Television', "1048770899000", 35900, 'tcl', None, '0006'),
                ('watch', "1748770988000", 45800, 'boat', None, '0007')]
        schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'product_number']
        df_test = spark.createDataFrame(data, schema)
        output_df = util_assignment1.time_stamp(df_test)
        self.assertEqual(output_df.collect(), test_time_stamp(df_test).collect())


if __name__ == '__main__':
    unittest.main()
