import os
import sys
#setting up environment variables - java_home, spark_home and pylib
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

#Importing SparkSession and SQL libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Utility functions for calculating total value of single order which will be used in UDF
#Taking type_multiplier here if order_type=Order then it should be 1 otherwise -1 for return
#Then calculating unit price of each product with unit quantity
#At the end returning the total order value from cost and type_multiplier.
def get_total_order_value (items, order_type):
    cost = 0
    type_multiplier = 1 if order_type == 'ORDER' else -1
    for item in items:
        cost = cost + (item['unit_price'] * item['quantity'])
        
    return cost * type_multiplier
	
#Utility functions for calculating total number of items present in single order which will be used in UDF
#Here we are calculating total_item_count with summing up quantity across all the product and returning total_count.
def get_total_item_count (items):
    total_count = 0
    for item in items:
        total_count = total_count + item['quantity']
    return total_count     

#Utility functions for is_order flag value for a single order
#Here returning 1 if it's order otherwise 0.
def get_order_flag(order_type):
    return 1 if order_type == 'ORDER' else 0


#Utility functions for is_return flag value for a single order
#here returning 1 if order type is return otherwise 0.
def get_return_flag(order_type):
    return 1 if order_type == 'RETURN' else 0

if __name__ == "__main__":

	#validating the command line arguments. Taking host_name, port and topic_name for validation.
	if len(sys.argv) != 4:
		print("Usage: spark-submit spark-streaming.py 18.211.252.152 9092 real-time-project")
		exit(-1)
		
	host = sys.argv[1]
	port = sys.argv[2]
	topic = sys.argv[3]
	
	#Initialize spark session, setting appName as retailOrderAnalyzer and log_level as ERROR - we can set Warning also but this is good practice to set ERROR in production.
	spark = SparkSession \
		.builder \
		.appName('retailOrderAnalyzer') \
		.getOrCreate() 
	spark.sparkContext.setLogLevel("ERROR")

	#Read Input from Kafka so we have to provide bootstrap_server name with port, offsets and subscribed topic_name.
	orderRaw = spark.readStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", "18.211.252.152" + ":" + "9092") \
		.option("startingOffsets", "latest") \
		.option("subscribe", "real-time-project") \
		.option("failOnDataLoss", False) \
		.load()

	#Define Schema of product Order with below columns name and each product contains SKU, title, unit_price and quantity.
	jsonSchema = StructType() \
		.add("invoice_no", LongType()) \
		.add("timestamp", TimestampType()) \
		.add("type", StringType()) \
		.add("country", StringType()) \
		.add("items", ArrayType(StructType([
		StructField("SKU", StringType()),
		StructField("title", StringType()),
		StructField("unit_price", DoubleType()),
		StructField("quantity", IntegerType())
	])))
	# Defining schema for reading from Kafka
	orderStream = orderRaw.select(from_json(col("value").cast("string"), jsonSchema).alias("data")).select("data.*")

	#Define the UDF with the utility functions as these are required for preprocessing the order of 4 extra attributes.
	add_total_order_value = udf(get_total_order_value, DoubleType())
	add_total_item_count = udf(get_total_item_count, IntegerType())
	add_order_flag = udf(get_order_flag, IntegerType())
	add_return_flag = udf(get_return_flag, IntegerType())

	#calculate additional columns which required for preprocessing are total_cost, total_items, is_order and is_return.
	expandedOrderStream = orderStream \
		.withColumn("total_cost", add_total_order_value(orderStream.items, orderStream.type)) \
		.withColumn("total_items", add_total_item_count(orderStream.items)) \
		.withColumn("is_order", add_order_flag(orderStream.type)) \
		.withColumn("is_return", add_return_flag(orderStream.type))
		
	#Write summarised input values in console. Writting in append mode on the console. Truncate mode is false as we don't want it to truncated.Returning data with 
	# processingTime = 1 minute interval and then start the job.
	extendedOrderQuery = expandedOrderStream \
		.select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
		.writeStream \
		.outputMode("append") \
		.format("console") \
		.option("truncate","false") \
		.trigger(processingTime="1 minute") \
		.start()
	
	#Calculating KPIs based on defined schema.
	#Calculate time-based KPIs
	#For Tumbling window, keeping time as 1 minute, delay threshold keeping the same 1 minute for watermark. Running aggregation sum of total_cost, average of total_cost
	#which is helping in calculating average_transaction_size. Count on Invoice_no which will give Order_Per_Minute count. Average of Is_return which will give rate_of_return.
	aggStreamByTime = expandedOrderStream \
		.withWatermark("timestamp", "1 minute") \
		.groupBy(window("timestamp", "1 minute", "1 minute")) \
		.agg(sum("total_cost"),
			 avg("total_cost"),
			 count("invoice_no").alias("OPM"),
			 avg("is_return")) \
		.select("window",
			"OPM",
			format_number("sum(total_cost)",2).alias("total_sale_volume"),
			format_number("avg(total_cost)",2).alias("average_transaction_size"),
			format_number("avg(is_return)",2).alias("rate_of_return"))
				
	#Calculate time and country based KPIs which is same like above but here we are getting time as well as country based KPIs data.
	aggStreamByTimeNCountry = expandedOrderStream \
		.withWatermark("timestamp", "1 minute") \
		.groupBy(window("timestamp", "1 minute", "1 minute"),"country") \
		.agg(sum("total_cost"),
			 count("invoice_no").alias("OPM"),
			 avg("is_return")) \
		.select("window",
			"country",
			"OPM",
			format_number("sum(total_cost)",2).alias("total_sale_volume"),
			format_number("avg(is_return)",2).alias("rate_of_return"))
				
	#Write time-based KPI values. Setting up in json format with append mode, no need to truncate and store these json files in folder - /tmp/op1 and
	#checkpoint location is /tmp/cp1.
	queryByTime = aggStreamByTime.writeStream \
		.format("json") \
		.outputMode("append") \
		.option("truncate", "false") \
		.option("path", "/tmp/op1") \
		.option("checkpointLocation", "/tmp/cp1") \
		.trigger(processingTime="1 minute") \
		.start()

	#Write time-based KPI values. Setting up in json format with append mode, no need to truncate and store these files in folder - /tmp/op2 and
	#checkpoint location is /tmp/cp2.
	queryByTimeNCountry = aggStreamByTimeNCountry.writeStream \
		.format("json") \
		.outputMode("append") \
		.option("truncate", "false") \
		.option("path", "/tmp/op2") \
		.option("checkpointLocation", "/tmp/cp2") \
		.trigger(processingTime="1 minute") \
		.start()
	#awaitTermination just wait for the termination signal from User.
	queryByTimeNCountry.awaitTermination()
