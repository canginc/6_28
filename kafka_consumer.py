### Temporary fork path, Not in usage
from kafka import KafkaConsumer
import redis

myRedis = redis.StrictRedis(  "localhost" , port=6379, db=0)

consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest',consumer_timeout_ms=7*1000)
consumer.subscribe(['GoodOutputTopic'])


#writing to redis
for message in consumer:
	myRedis.set(message.key,message.value)
	if (message != None):
		print("@@@ KAFKA CONSUMER @@@ writing to redis string(msg)=[" + str(message) +"]" )

	pass
