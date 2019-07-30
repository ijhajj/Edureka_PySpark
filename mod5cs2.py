import re
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
spark=SparkSession.builder.appName("Module 5 SparkSession take").getOrCreate()
access_logs_DF = spark.read.text("/user/edureka_524533/Datasets/access.clean.log")
ts_pattern = r'\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}'
dt_pattern = r'\d{2}/\w{3}/\d{4}'
ht_pattern=r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
byt_pattern=r'\s\d{4,}\s'
meth_pattern =r'\s[A-Z]{3,}\s'
Url_pattern = r'\s/(\S+)'
status_pattern = r'\s(\d{3})\s'
result = access_logs_DF.withColumn('Timestamp', regexp_extract(col('value'), ts_pattern, 0))
result = result.withColumn('Date', regexp_extract(col('TimeStamp'), dt_pattern, 0))
result = result.withColumn('Host', regexp_extract(col('value'), ht_pattern, 0))
result = result.withColumn('Bytes', regexp_extract(col('value'), byt_pattern, 0))
result = result.withColumn('Method', regexp_extract(col('value'), meth_pattern, 0))
result = result.withColumn('URL', regexp_extract(col('value'), Url_pattern, 0))
result = result.withColumn('StatusCode', regexp_extract(col('value'), status_pattern, 1))
# Verify there are no null columns in the original dataset.
bad_rows_df = result.filter(result['Host'].isNull()|result['Timestamp'].isNull()|result['Method'].isNull()|result['StatusCode'].isNull()|result['Bytes'].isNull()|result['Date'].isNull())
nullCount = bad_rows_df.count()
print("Number of null columns")
print(str(nullCount))
#Describe which HTTP status values appear in data and how many
StatusCodeDF = result.select(result['StatusCode'])
StatusCodeDF=StatusCodeDF.dropDuplicates()
StatusCodeDF.show()
StatusCount = StatusCodeDF.count()
print("Number of unique status codes")
print(str(StatusCount))
#How many unique hosts are there in the entire log and their average request
HostsDF = result.select(result['Host'],result['Bytes'])
HostsDF = HostsDF.groupby('Host').agg(F.mean('Bytes'),F.count('Bytes').alias('AverageRequests'))
HostsDF.show()
HostCount = HostsDF.count()
print("Number of Unique Hosts")
print(str(HostCount))
# Find out how many 404 HTTP codes are in access logs
Count404 = result.where(result['StatusCode']=='404').count()
print("Total 404 HTTP codes")
print(str(Count404))

