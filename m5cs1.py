from __future__ import division
from pyspark import SparkContext,SparkConf
from pyspark.sql.session import SparkSession
from pyspark.mllib.stat import Statistics
import pandas as pd
import pyspark.sql.functions as sqlf
from pyspark.sql import SQLContext


conf=SparkConf().setAppName("Module5 CaseStudy 1").set("spark.hadoop.yarn.resourcemanager.address","192.168.0.104:8032")
sc=SparkContext(conf=conf)
sparkSess=SparkSession(sc)

#Read AppStore.csv file
appCSVData = sparkSess.read.csv("/user/edureka_524533/Datasets/AppleStore.csv",header=True,inferSchema=True)

#Convert bytes to MB and GB in a new column
appCSVGBData=appCSVData.withColumn("size_gigabytes",appCSVData[3]*9.3132257461548E-10)
appCSVMBData=appCSVGBData.withColumn("size_megabytes",appCSVGBData[3]*9.5367431640625E-7)

#Convert the DataFrame to rdd
appCSVRDD=appCSVMBData.rdd

#List top 10 trending apps
appRatingRDD=appCSVRDD.map(lambda line:(line[6],line[2]))
topTenTrendingApps=appRatingRDD.sortByKey(ascending=False)
topTenTrendingApps.take(10)

#The difference in the average number of screenshots displayed of highest and lowest rating apps
#Highest Rating app = with user_rating = 5.0
#Lowest Rating app = with user_rating = 0.0
#Find the average screen shots (ipadSc_urls.num) belonging to highest rating app
#Find the average screen shots (ipadSc_urls.num) belonging to lowest rating app
#Then find the difference
ratingRDD=appCSVRDD.map(lambda data:data[8])
maxRatingRDD=appCSVRDD.filter(lambda data:data[8]==5.0)
maxRatingScreenShotRDD=maxRatingRDD.map(lambda data:data[14])
maxScrShotVal = maxRatingScreenShotRDD.mean()
minRatingRDD=appCSVRDD.filter(lambda data:data[8]==0.0)
minRatingScreenShotRDD=minRatingRDD.map(lambda data:data[14])
minScrShotVal = minRatingScreenShotRDD.mean()
diff = maxScrShotVal - minScrShotVal
print(str(diff))

#What percentage of high rated apps support multiplelanguages
#Total no of high rated apps
#Total no of high rated apps with multiple languages support i.e. 'lang.num'>1
#Percentage = (Total with multi support/Total)*100
TotalHRated=maxRatingRDD.count()
maxRatingMultiLangRDD = maxRatingRDD.filter(lambda data:data[15]>1)
TotalHRatedMultiLang = maxRatingMultiLangRDD.count()

percentageMaxRatedMultiLang = (TotalHRatedMultiLang/TotalHRated)*100
print(percentageMaxRatedMultiLang)

# Does length of app description contribute to the ratings?

sqlContext = SQLContext(sc)
app_desc = pd.read_csv("datasets/appleStore_description.csv")
appCSVDescDF = sqlContext.createDataFrame(app_desc)
appCSVDescRDD = appCSVDescDF.rdd
appIDDescRDD=appCSVDescRDD.map(lambda data:(data[0],len(data[3])))
appIDRatingRDD = appCSVRDD.map(lambda data:(data[1],data[8]))
appRatingDescRDD = appIDRatingRDD.join(appIDDescRDD)
axisX=appRatingDescRDD.map(lambda data:data[1][0])
axisY=appRatingDescRDD.map(lambda data:data[1][1])
Statistics.corr(axisX, axisY, method="pearson")

# Compare the statistics of different app groups/genres
#genres,user_rating,sup_devices.num,ipadSc_urls.num,lang.num
CSVRDDByGenre = appCSVRDD.map(lambda data:(data[12],data[8],data[13],data[14],data[15]))
CSVDF=CSVRDDByGenre.toDF(["Genre","Rating","SupportDevices","ScreenShots","NumberLanguages"])

CSVDF.groupBy("Genre").agg(sqlf.var_pop("NumberLanguages"),sqlf.corr("Rating","SupportDevices"),sqlf.avg("ScreenShots"),sqlf.mean("NumberLanguages")).show()

