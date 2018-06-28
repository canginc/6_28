"""
### Structured streaming code , temporary fork path and  not in used
### reading from Kafka topic
@output to console 
"""
import sys
from pyspark import SparkContext
from pyspark.context  import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StructType
import time
from kafka import KafkaConsumer
import redis
from os.path import expanduser, join, abspath


def main():

	warehouse_location = abspath('spark-warehouse')

	spark = SparkSession\
		.builder\
		.appName("##WorkingGoodStreaming##")\
		.config("spark.sql.warehouse.dir", warehouse_location) \
		.enableHiveSupport() \
		.getOrCreate()

	# Create DataFrame readaing stream of lines by subscribing # Dataset<Row>  load(String path)
	datafrm = spark.readStream\
		.format("kafka")\
		.option("kafka.bootstrap.servers", "localhost:9092")\
		.option("startingOffsets", "latest")\
		.option("failOnDataLoss", "false")\
		.option("subscribe", "GoodInTopic")\
		.load()

	##hoa #datafrm = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("failOnDataLoss", "false").option("subscribe", "GoodInTopic").load()
#datafrm = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ec2-34-224-164-5.compute-1.amazonaws.com:9092").option("subscribe", "GoodInTopic").load()
	#datafrm = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("startingOffsets", "latest").option("subscribe", "GoodInTopic").load()
	# Dataset<Row>  load(String path)  #datafrm.printSchema()

	# public Dataset<Row> selectExpr(scala.collection.Seq<String> exprs)	#Select key:value and discard others
	datafrm = datafrm.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

	t1 = datafrm.select("value")
	# table1 t1 of type  Dataset<Row>;	 return public Dataset<Row> select(Column... cols)
	split_col = functions.split( datafrm['value'], ',')

	t1 = t1.withColumn('ISIN', split_col.getItem(0))
	t1 = t1.withColumn('Mnemonic', split_col.getItem(1))
	t1 = t1.withColumn('SecurityDesc', split_col.getItem(2))
	t1 = t1.withColumn('SecurityType', split_col.getItem(3))
	t1 = t1.withColumn('Currency', split_col.getItem(4))
	t1 = t1.withColumn('SecurityID', split_col.getItem(5))
	t1 = t1.withColumn('Date', split_col.getItem(6))
	t1 = t1.withColumn('Time', split_col.getItem(7))
	t1 = t1.withColumn('StartPrice', split_col.getItem(8))
	t1 = t1.withColumn('MaxPrice', split_col.getItem(9))
	t1 = t1.withColumn('MinPrice', split_col.getItem(10))
	t1 = t1.withColumn('EndPrice', split_col.getItem(11))
	t1 = t1.withColumn('TradedVolume', split_col.getItem(12))
	t1 = t1.withColumn('NumberOfTrades', split_col.getItem(13))
	t1 = t1.withColumn('Swing_MaxLessMinPrice', ( split_col.getItem(9) - split_col.getItem(10) )  )
	t1 = t1.drop('value')

	t1.printSchema()

	print ( "@@ Table 1 t1 dat type is=" + type(t1 ).__name__)

	#data_stream_transformed = t1.groupBy("ISIN").agg(sum("TradedVolume"), max( "Swing_MaxLessMinPrice"))
	#print ( "### data_stream_transformed is=" + type(  data_stream_transformed ).__name__)
	#data_stream_transformed.printSchema()

	# returns highest volatility streaming dataframe
	#sending output stock-groupBy-summary data  to Kafka's Aggregated output tpc

	#ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
#"AT0000609607","ABS2","PORR AG","Common stock","EUR",2504166,2018-06-19,11:00,28.85,28.85,28.8,28.85,100,2
#"LU0378438732","C001","COMSTAGE-DAX UCITS ETF I","ETF","EUR",2504271,2018-06-19,11:00,119.96,119.96,119.96,119.96,1650,2

## Strange VOLUME 
	#query = fileStreamDf.writeStream.option("checkpointLocation", '/tmp/check_point/')\
		#.format("org.apache.spark.sql.cassandra").option("keyspace", "germany").option("table", "test").start()
	# WELL working ###	
	#largestVlt = spark.sql("select  SecurityDesc as key, max(Swing_MaxLessMinPrice) as value from Germany where (Swing_MaxLessMinPrice > 0.1) group by SecurityDesc     order by max(Swing_MaxLessMinPrice) desc")

#	vltquery = largestVlt.selectExpr("CAST(key AS STRING)", "CAST(value  AS STRING)" ).writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "GoodOutputTopic").option("checkpointLocation", "hdfs://ec2-34-224-164-5.compute-1.amazonaws.com:9870/tmp").outputMode("complete").start()  

	## working ## largestVlt = spark.sql("select  ISIN, Mnemonic, SecurityDesc as key, max(Swing_MaxLessMinPrice) as value , max(TradedVolume), max(NumberOfTrades)  from Germany where ROWNUM < 11 and (Swing_MaxLessMinPrice > 0.1) group by ISIN,  Mnemonic, SecurityDesc   order by max(Swing_MaxLessMinPrice) desc")
	## working ## 	#vltquery = largestVlt.writeStream.outputMode("complete").format("console").option('truncate', 'false').option('numRows', '22').start()

	#largestVlt = spark.sql("select  ISIN, Mnemonic, SecurityDesc as key, max(Swing_MaxLessMinPrice) as value , max(TradedVolume), max(NumberOfTrades)  from Germany where ROWNUM < 11 and (Swing_MaxLessMinPrice > 0.1) group by ISIN,  Mnemonic, SecurityDesc   order by max(Swing_MaxLessMinPrice) desc")

	t1.createOrReplaceTempView("Germany")

	largestVlt = spark.sql("select  SecurityDesc as key, max(Swing_MaxLessMinPrice) as value  \
		from Germany \
		where (min(Swing_MaxLessMinPrice) > 3) \
		group by ISIN,  Mnemonic, SecurityDesc   \
		order by max(Swing_MaxLessMinPrice) desc ")

	vltquery = largestVlt.selectExpr("CAST( key AS STRING)", "CAST( value  AS STRING)" )\
		.writeStream.format("kafka")\
		.option("kafka.bootstrap.servers", "localhost:9092")\
		.option("topic", "GoodOutputTopic")\
		.option("checkpointLocation", "/home/ubuntu/temp")\
		.outputMode("complete")\
		.start()  

	vltquery.awaitTermination()

if __name__ == '__main__':
    main()

	# WELL working ###	largestVlt = spark.sql("select  SecurityDesc as key, max(Swing_MaxLessMinPrice) as value from Germany where (Swing_MaxLessMinPrice > 0.1) group by SecurityDesc     order by max(Swing_MaxLessMinPrice) desc")
	# WELL working ### vltquery = largestVlt.writeStream.outputMode("complete").format("console").option('truncate', 'false').option('numRows', '100').start()

	# working ###largestVlt = spark.sql("select  ISIN, Mnemonic, SecurityDesc, max(Swing_MaxLessMinPrice) , max(TradedVolume), max(NumberOfTrades)  from Germany where (Swing_MaxLessMinPrice > 0.1) group by ISIN,  Mnemonic, SecurityDesc   order by max(Swing_MaxLessMinPrice) desc")
	# working ## vltquery = largestVlt.writeStream.outputMode("complete").format("console").option('truncate', 'false').option('numRows', '100').start()
