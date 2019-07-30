from __future__ import division
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("Module 6 Case Study 1").getOrCreate()
ordersDF = spark.read.csv("/user/edureka_524533/Datasets/orders.csv",inferSchema=True,header=True)
# Divide Order DF into three : Prior data, Train data, Test Data
orderPriorDF = ordersDF.where(ordersDF['eval_set']=="prior")
orderTrainDF = ordersDF.where(ordersDF['eval_set']=="train")
orderTestDF = ordersDF.where(ordersDF['eval_set']=="test")
# Now Join each individual Order DF with its counter orders_product*.csv DF
priorDF=spark.read.csv("/user/edureka_524533/Datasets/order_products__prior.csv",inferSchema=True,header=True)
orderPriorJoinDF = orderPriorDF.join(priorDF,on=['order_id'],how='left_outer')
trainDF = spark.read.csv("/user/edureka_524533/Datasets/order_products__train.csv",inferSchema=True,header=True)
orderTrainJoinDF = orderTrainDF.join(trainDF,on=['order_id'],how='left_outer')
#We do not have the test product details, so either we can simply ignore them or go ahead to create a join with nulls
orderTestJoinDF = orderTestDF.join(trainDF,on=['order_id'],how='left_outer')
# Now Join all the three OrderDFs: orderPriorJoin, orderTrainJoin, orderTestJoin
Orders1 = orderPriorJoinDF.unionAll(orderTrainJoinDF)
ordersAllDF=Orders1.unionAll(orderTestJoinDF)
# Now we need tp Join this DF with Products.csv
prodDF=spark.read.csv("/user/edureka_524533/Datasets/products.csv",inferSchema=True,header=True)
orderProductAllDF = ordersAllDF.join(prodDF,on='product_id',how='left_outer')
# Now Join the aisle details using aisles.csv
aisleDF=spark.read.csv("/user/edureka_524533/Datasets/aisles.csv",inferSchema=True,header=True)
orderProductAisleAllDF = orderProductAllDF.join(aisleDF,on='aisle_id',how='left_outer')
# Now join the department details as well using departments.csv
deptDF=spark.read.csv("/user/edureka_524533/Datasets/departments.csv",inferSchema=True,header=True)
OrderProductAisleDepartAllDF = orderProductAisleAllDF.join(deptDF,on='department_id',how='left_outer')
OrderProductAisleDepartAllDF.show(5)
# Check missing data
missingDataDF = OrderProductAisleDepartAllDF.filter(OrderProductAisleDepartAllDF['department_id'].isNull()|
                                                   OrderProductAisleDepartAllDF['aisle_id'].isNull()|
                                                   OrderProductAisleDepartAllDF['product_id'].isNull()|
                                                   OrderProductAisleDepartAllDF['order_id'].isNull()|
                                                   OrderProductAisleDepartAllDF['user_id'].isNull()|
                                                   OrderProductAisleDepartAllDF['eval_set'].isNull()|
                                                   OrderProductAisleDepartAllDF['order_number'].isNull()|
                                                   OrderProductAisleDepartAllDF['order_dow'].isNull()|
                                                   OrderProductAisleDepartAllDF['order_hour_of_day'].isNull()|
                                                   OrderProductAisleDepartAllDF['days_since_prior_order'].isNull()|
                                                   OrderProductAisleDepartAllDF['add_to_cart_order'].isNull()|
                                                   OrderProductAisleDepartAllDF['reordered'].isNull()|
                                                   OrderProductAisleDepartAllDF['product_name'].isNull()|
                                                   OrderProductAisleDepartAllDF['aisle'].isNull()|
                                                   OrderProductAisleDepartAllDF['department'].isNull())
missingDataCount = missingDataDF.count()
print("Total missing data")
print(str(missingDataCount))
# List the most ordered products (top 10)
gpByProductDF = OrderProductAisleDepartAllDF.groupby('product_name').count()
Top10OrderedProducts = gpByProductDF.orderBy(col('count').desc()).limit(10)
print("Top 10 Most Ordered Products")
Top10OrderedProducts.show()
# Do people usually reorder the same previous ordered products?
reorderedCount = OrderProductAisleDepartAllDF.where(OrderProductAisleDepartAllDF['reordered']==1).count()
notReorderedCount = OrderProductAisleDepartAllDF.where(OrderProductAisleDepartAllDF['reordered']==0).count()
# We are ignoring the 75000 test records for which we do not have the order_product__test.csv data
Total = reorderedCount + notReorderedCount
reorderPercentile = (reorderedCount/Total)*100
print("Percentage of people usually reordering the same previous ordered product")
print(str(reorderPercentile))
# List most reordered products
reorderedProdDF = OrderProductAisleDepartAllDF.where(OrderProductAisleDepartAllDF['reordered']==1)
reorderedGpByProdDF = reorderedProdDF.groupby('product_name').count()
print("Top 10 most  Reordered Products")
Top10ReorderedProducts = reorderedGpByProdDF.orderBy(col('count').desc()).limit(10)
Top10ReorderedProducts.show()
# Most important aisle (by number of products)
totProdPerAisle = prodDF.groupby('aisle_id').count()
val = totProdPerAisle.agg({'count':'max'}).collect()[0][0]
totProdPerAisle.createOrReplaceTempView("AisleCountTab")
aisle_id = spark.sql("Select * from AisleCountTab where count = {0}".format(val)).collect()[0][0]
mostImpAisle = aisleDF.where(aisleDF['aisle_id']==aisle_id)
print("Most important Aisle by number of products")
mostImpAisle.show()
#Looking for next most important aisle as 100 is missing from aisle.csv
nextTotProdPerAisleDF = totProdPerAisle.where(totProdPerAisle['aisle_id']!=aisle_id)
nextval = nextTotProdPerAisleDF.agg({'count':'max'}).collect()[0][0]
nextTotProdPerAisleDF.createOrReplaceTempView("NextAisleCountTab")
aisle_id = spark.sql("Select * from NextAisleCountTab where count = {0}".format(nextval)).collect()[0][0]
nextMostImpAisle = aisleDF.where(aisleDF['aisle_id']==aisle_id)
print("Next Most important Aisle")
nextMostImpAisle.show() 
# Most important department (by number of products)
totProdPerDeptDF = prodDF.groupby('department_id').count()
maxCount = totProdPerDeptDF.agg({'count':'max'}).collect()[0][0]
totProdPerDeptDF.createOrReplaceTempView("DeptCountTab")
dept_id = spark.sql("Select * from DeptCountTab where count = {0}".format(maxCount)).collect()[0][0]
mostImpDept = deptDF.where(deptDF['department_id']==dept_id)
print("Most important deparatment by number of products")
mostImpDept.show()
# Get the Top 10 departments
totProdOrderedPerDeptDF = OrderProductAisleDepartAllDF.groupby('department').count()
Top10Departments = totProdOrderedPerDeptDF.orderBy(col('count').desc()).limit(10)
print("Top Ten Departments")
Top10Departments.show()
# List top 10 products ordered in the morning (6 AM to 11 AM)
morningOrdersDF=OrderProductAisleDepartAllDF.where((OrderProductAisleDepartAllDF['order_hour_of_day']>=6)&(OrderProductAisleDepartAllDF['order_hour_of_day']<=11))
totProdOrderedMornDF = morningOrdersDF.groupby('product_name').count()
Top10MornProducts = totProdOrderedMornDF.orderBy(col('count').desc()).limit(10)
print("Top 10 Morning Products")
Top10MornProducts.show()




