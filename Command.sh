#!/bin/bash

/home/ubuntu/prj/workSrc

python  kafka_producer.py  > lg1

/usr/local/spark/bin/spark-submit --master spark://ec2-34-224-164-5.compute-1.amazonaws.com:7077  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0    --packages  org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2     cassanSpark.py    > log2

## WORKING
##ubuntu@ip-10-0-0-9:~/prj/workSrc$ 
/usr/local/spark/bin/spark-submit --master spark://ec2-34-224-164-5.compute-1.amazonaws.com:7077  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0    --packages  org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2     fix_annaDrink_caSpark.py    > log2

# wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.1.1/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar

###ubuntu@ip-10-0-0-9:/usr/local/spark/jars$ wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.1.1/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar
#--2018-06-27 15:53:29--  http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.1.1/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar
#Resolving central.maven.org (central.maven.org)... 151.101.32.209
#Connecting to central.maven.org (central.maven.org)|151.101.32.209|:80... connected.
#HTTP request sent, awaiting response... 200 OK
#Length: 11297664 (11M) [application/java-archive]
#Saving to: ‘spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar’
#
#spark-streaming-kafka-0-8-assembly_2.11 100%[============================================================================>]  10.77M  62.8MB/s    in 0.2s
#
#2018-06-27 15:53:30 (62.8 MB/s) - ‘spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar’ saved [11297664/11297664]

cqlsh:playground> select * from tableinsight limit 7;



ubuntu@ip-10-0-0-9:~/prj/workSrc$ pwd

ubuntu@ip-10-0-0-9:~/prj/workSrc$ 

/usr/local/spark/bin/spark-submit --master spark://ec2-34-224-164-5.compute-1.amazonaws.com:7077  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0  structuredStream.py   >  lg2


### Working ###
### Working ###
hdfs dfs -rm   /home/ubuntu/prj/ingestion/*.*
hdfs dfs -ls   /home/ubuntu/prj/ingestion/*.*

cd    /home/ubuntu/prj/ingestion/
echo "Start Kafka producer"
python  kafka_producer.py &
PROD_PID=$!
sleep 21

echo "Start Kafka consumer"
python  kafka_consumer.py &
CONS_PID=$!sleep $7

hdfs dfs -ls   /home/ubuntu/prj/ingestion/

### CAREFUL!!!!!! create topic only once and leave it alone
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 3 --topic GoodInTopic

/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 3 --topic GoodOutTopic

hdfs    dfs -ls /

##################################################################
# ubuntu@ip-10-0-0-9:~/src$ /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 3 --topic GoodInTopic
#Created topic "GoodInTopic".

#ubuntu@ip-10-0-0-9:~/src$ /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 3 --topic GoodOutTopic
#Created topic "GoodOutTopic".
#ubuntu@ip-10-0-0-9:~/src$ /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
########AggtOutputTopic
#GoodInTopic
#GoodOutTopic
########InputTopic
#########MyTopic
########RawInputTopic
########__consumer_offsets
########ubuntu@ip-10-0-0-9:~/src$


##################################################################
# /usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 3 --topic RawInputTopic
#/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic RawInputTopic
#/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic   AggtOutputTopic
