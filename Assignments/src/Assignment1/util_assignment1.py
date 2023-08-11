from pyspark.sql.functions import *

def time_stamp(df):
    """
    Convert the Issue Date with the timestamp format
    # """
    # df.select(col('Issue Date'), to_timestamp(col('Issue Date'))).show(truncate=False)
    timestamp = df.withColumn('NewTime', from_unixtime(col('Issue_Date') / 1000, "yyyy-MM-dd 'T' HH:mm:ssZZZZ"))
    # """
    # Convert timestamp to date type
    # """
    datetype = timestamp.withColumn("Date", split(col('NewTime'), 'T')[0])
    # datetype.show()
    # """
    # Remove extra space
    # """
    trim1 = datetype.withColumn('Brands', ltrim(col('Brand')))
    # trim1.show(truncate=False)
    # """
    # Replace null values with empty values in Country column
    # """
    tr = trim1.fillna("Empty")
    return tr


# def transform(df1):
#     rename = df1.withColumnRenamed('Product Name', 'product name') \
#         .withColumnRenamed('Brand', 'brand') \
#         .withColumnRenamed('Language', 'language') \
#         .withColumnRenamed('ModelNumber', 'model_number') \
#         .withColumnRenamed('StartTime', 'start_time') \
#         .withColumnRenamed('Product Number', 'product number')
#     # rename.show(truncate=False)
#     df4 = rename.withColumn("start_time_ms", unix_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ") * 1000)
#     df4.show(truncate=False)









# spark = SparkSession.builder.getOrCreate()
# data = [('Washing Machine', "1648770933000", 20000, 'samsung', 'India', '0001'),
#                 ('Refrigerator', "1648770999000", 35000, 'LG', None, '0002'),
#                 ('Air Cooler', "1648770948000", 45000, 'Voltas', None, '0003')]
# schema = ['Product Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'Product_Number']
# df = spark.createDataFrame(data, schema)
# data1 = [(150711, 123456, 'EN', 456789, '2021-12-27T08:20:29.842+0000', '0001'),
#                  (150439, 234567, 'UK', 345678, '2021-12-27T08:21:14.645+0000', '0002'),
#                  (150647, 345678, 'ES', 234567, '2021-12-27T08:22:42.445+0000', '0003')]
# schema1 = ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'Product_Number']
# df1 = spark.createDataFrame(data1,schema1)
# df1.join(df,df1.Product_Number == df.Product_Number,'inner').show()
# # empDf.join(depDf, empDf.dep == depDf.id,'full').show()


