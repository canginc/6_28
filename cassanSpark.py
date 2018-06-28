"""
This file reads pricing volatility data from Kafka Topic 
and save the results to Cassandra table.

Input:  "14-fields string" from  Topic
	cleanup string by removing field indices [1,3,5]
	reading "14-fields string" from  Germany Topic:  
		Input Format:
		ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades


output: saving pricing data   to   Cassandra database tableinsight
		Output Format to Cassandra:
 		ISIN,       SecurityDesc,           Currency,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
		save  the "11-fields string" to Cassandra
"""
from __future__ import print_function
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
import decimal
import sys
import time
import threading
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import functions
from pyspark.sql.functions import split

from cassandra.cluster import Cluster


####################################################################
###
### convert input "14 fields (comma separated) string" to output "11-fields list"
### example of original comma-separated string :
### ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
# "CH0038863350","NESR","NESTLE NAM.        SF-,10","Common stock","EUR",2504245,2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
# "CH0303692047","ED4","EDAG ENGINEERING G.SF-,04","Common stock","EUR",2504254,2018-06-22,11:00,17.9,17.9,17.9,17.9,4,1
#
### remove  Mnemonic,SecurityType,SecurityID
### Now schema contains
### ISIN,	SecurityDesc,		Currency,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
# "CH0038863350","NESTLE NAM.        SF-,10","EUR",2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
####################################################################
def extract_data( instring):

	#print ( "# extract_data  type of instring="+ str(type( instring) ) )
	# extract_data  type of instring=<type 'unicode'>

        listTokens = instring.split( ',')
	#print("###  showing listTokens=" + str( listTokens ) )

	# initialize output
	output = range(11)	# return list of 11 items=[0,1,2,....10]
	#print("###  initialized  output=" + str(output ) )

	## for rdd
        output[0] =listTokens[0]	#'ISIN'

	if (listTokens[2] is not None):
        	output[1] =listTokens[2]	#'SecurityDesc', 

	if (listTokens[4] is not None):
        	output[2] = listTokens[4]	#Currency', 

	if (listTokens[6] is not None):
        	output[3] = listTokens[6]	#Date', 

	if (listTokens[7] is not None):
        	output[4] = listTokens[7]	#Time', 
	if (listTokens[8] is not None):
        	output[5] = listTokens[8]	#StartPrice', 
	if (listTokens[9] is not None):
        	output[6] = listTokens[9]	#'MaxPrice', 
	if (listTokens[10] is not None):
        	output[7] = listTokens[10]	#MinPrice', 
	if (listTokens[11] is not None):
        	output[8]=  listTokens[11]	#EndPrice', 
	if (listTokens[12] is not None):
        	output[9]= listTokens[12] 	#'TradedVolume', 
	if (listTokens[13] is not None):
        	output[10] = listTokens[13]	#NumberOfTrades', 

	""""
	### for dataframe  in STRUCTURED streaming
        output = output.append ('ISIN', split_col.getItem(0))
        output = output.append('SecurityDesc', split_col.getItem( 2 ))
        output = output.append('Currency', split_col.getItem( 4 ))
        output = output.append('Date', split_col.getItem(6))
        output = output.append('Time', split_col.getItem(7))
        output = output.append('StartPrice', split_col.getItem( 8 ))
        output = output.append('MaxPrice', split_col.getItem( 9 ))
        output = output.append('MinPrice', split_col.getItem(10 ))
        output = output.append('EndPrice', split_col.getItem( 11 ))
        output = output.append('TradedVolume', split_col.getItem( 12 ))
        output = output.append('NumberOfTrades', split_col.getItem( 13 ))
        #t1 = t1.withColumn('Swing_MaxLessMinPrice', ( split_col.getItem(9) - split_col.getItem(10) )  )
	"""
	return output



####################################################################
### start processs
### Now schema contains
### ISIN,	SecurityDesc,		Currency,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
# "CH0038863350","NESTLE NAM.        SF-,10","EUR",2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
####################################################################
def process(time, rdd):
    
	if (rdd is None) : 
		return
	num_of_record = rdd.count()
	if  num_of_record ==0:
		return 

	lines = rdd.map(lambda somestruct: somestruct[1])   

	if  (lines is None):
		print("**** TROUBLE empty  @@@ lines = rdd.map(lambda struct: struct[1]) ")
		return

	# for each line in the dataset
	for each in lines.collect():
		# convert each line of comma-separated STRING into LIST
		pricing = extract_data(each)
		print (" @@ loop each pricing =" + str(pricing ) +"]" )

		# structured streaming  (pricing is None or pricing['ISIN']==' ' or pricing['ISIN']==''):
		if (pricing is None or pricing[0]==' ' or pricing[0]==''):
			continue

		# REMINDER: table created only once
		# create_table = session.prepare("CREATE TABLE IF NOT EXISTS tableinsight (ISIN text PRIMARY KEY, SecurityDesc text, Currency text, Date text, Time text, StartPrice float, MaxPrice float, MinPrice float, EndPrice float, TradedVolume int, NumberOfTrades int );")

		session.execute(  to_cassandra,  \
			(pricing[0], pricing[1], pricing[2],\
			pricing[3], pricing[4], float(pricing[5]) , \
			float(pricing[6]), \
			float(pricing[7]), \
			float(pricing[8]), \
			int( pricing[9]),  \
			int(pricing[10])) 	)

			# for structured streaming
			#(pricing['ISIN'], pricing['SecurityDesc'], pricing['Currency'],\
			#pricing['Date'], pricing['Time'], pricing['StartPrice'], \
			#pricing['MaxPrice'], pricing['MinPrice'], pricing['EndPrice'],  \
			#pricing['TradedVolume'], pricing['NumberOfTrades']))
    #END for loop insertign each LINE of RDD into cassandra table 
### END process function
### end processs



####################################################################
####################################################################
### MAIN ###########################################################
####################################################################
####################################################################
### ORIGINALly
### ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
# "CH0038863350","NESR","NESTLE NAM.        SF-,10","Common stock","EUR",2504245,2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
# "CH0303692047","ED4","EDAG ENGINEERING G.SF-,04","Common stock","EUR",2504254,2018-06-22,11:00,17.9,17.9,17.9,17.9,4,1
#
###  ***Sending to GoodInTopic key=["AT0000A0E9W5"] and text=["AT0000A0E9W5","SANT","S+T AG (Z.REG.MK.Z.)O.N.","Common stock","EUR",2504159,2018-06-22,08:00,21.94,21.94,21.94,21.94,636,3]
#
# "CH0038863350","NESR","NESTLE NAM.        SF-,10","Common stock","EUR",2504245,2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
# "CH0038863350","NESR","NESTLE NAM.        SF-,10","Common stock","EUR",2504245,2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
#
### remove  Mnemonic,SecurityType,SecurityID
### Now schema contains
### ISIN,	SecurityDesc,		Currency,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades
# "CH0038863350","NESTLE NAM.        SF-,10","EUR",2018-06-22,11:00,64.46,64.46,64.46,64.46,7,1
#####################################
## only created once in  Cassandra node
# ubuntu@ip-10-0-0-7:
# create_table = session.prepare("CREATE TABLE IF NOT EXISTS tableinsight (ISIN text PRIMARY KEY, SecurityDesc text, Currency text, Date text, Time text, StartPrice float, MaxPrice float, MinPrice float, EndPrice float, TradedVolume int, NumberOfTrades int );")
#####################################
# session.execute(create_table)
#

if __name__ == "__main__":
	cassandra_master = "ec2-34-204-117-202.compute-1.amazonaws.com"
	#if multiple nodes exist for cassandra, append your borkers as a list here
	keyspace = "playground"
	cluster = Cluster([cassandra_master])
	session = cluster.connect(keyspace)

	to_cassandra = session.prepare("INSERT INTO tableinsight (ISIN, SecurityDesc, Currency, Date, Time, StartPrice, MaxPrice, MinPrice, EndPrice, TradedVolume, NumberOfTrades) VALUES (?,?,?,?,?,?,?,?,?,?,?)")

#kafkabrokers = "your_aws_cluster_public_DNS:9092"
#conf = SparkConf().setAppName("PySpark Cassandra").set("spark.es.host", "ec2-52-88-7-3.us-west-2.compute.amazonaws.com").set("spark.streaming.receiver.maxRate",2000).set("spark.streaming.kafka.maxRatePerPartition",1000).set("spark.streaming.backpressure.enabled",True).set("spark.cassandra.connection.host","172.31.1.138")
#
#kafkabrokers = ["ec2-34-224-164-5.compute-1.amazonaws.com:9092", "ec2-52-1-193-73.compute-1.amazonaws.com:9092", "ec2-52-20-254-67.compute-1.amazonaws.com:9092", "ec2-18-207-59-207.compute-1.amazonaws.com:9092"] 
	kafkabrokers = "ec2-34-224-164-5.compute-1.amazonaws.com:9092"
	readingFromTopicName = "GoodInTopic"

	sc = SparkContext(appName="PriceSwing")
	ssc = StreamingContext(sc, 7)

	#get stream data from kafka
	kafkaStream = KafkaUtils.createDirectStream(ssc, topics=[ readingFromTopicName], kafkaParams={"metadata.broker.list": kafkabrokers})

	kafkaStream.foreachRDD(process) 
	ssc.start()
	ssc.awaitTermination()

