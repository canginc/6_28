REAMDE

Live Data from S3 bucket were processed with 2 branches:


########################################### TOP BRANCH ######################
S3 -> Kafka Topic -> Spark streaming (normal) ->  Cassandra
	kafka_producer.py	
	cassanSpark.py  
	Command.sh  
	lg1  log2  

########################################### BOTTOM  BRANCH ######################
S3 -> Kafka Topic -> Spark structured streaming  -> Redis
	kafka_producer.py
	structuredStream.py
	kafka_consumer.py  
  
