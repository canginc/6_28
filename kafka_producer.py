"""
This producer reads AWS S3 bucket data, 
Cleans the Germany data from S3 bucket,
sends these  Germany  data to Kafka Topic    GoodInTopic

"""
###############################################################################
import random
import os
import sys
import datetime
import time
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from kafka.producer import KafkaProducer
from kafka.client import KafkaClient
import boto

if sys.version_info < (3,):
    from cStringIO import StringIO
else:
    from io import StringIO
    xrange = range
from tokenize import generate_tokens

###
### this is  an inefficient way to clean strings; 
### input: string with quote-enclosed comma 
### output: replaced all quote-enclosed comma with UNDERSCORE
###
def old_clean_parts ( instring ):
        openQuote = false
        outlist =[]
        for char in instring:
                # toggling open quote" and closed quote"
                if char=='"':
                        openQuote = not openQuote

                if char==',' and openQuote:
                        outlist.append('_')
                else:
                        outlist.append( char)

"""
@ input string containing Quote-enclosed commas
@ output string: replaces all  Quote-enclosed commas  with  Quote-enclosed UNDERSCORE
#
 line schema contains 14 commaSeparate fields:  CAVEAT some field may contain comma. Use "" to identify fields with comma
	dataframe [ISIN,Mnemonic,SecurityDesc,SecurityType,Currency,SecurityID,Date,Time,StartPrice,MaxPrice,MinPrice,EndPrice,TradedVolume,NumberOfTrades ]
 	each field is delimited by openQuote and closedquote

# cleaning Data: certain field SecurityDesc contains COMMA inside openQuote and closedQuote
# "GB0059822006","DLG","DIALOG SEMICOND.   LS-,10","Common stock","EUR",2505387,2018-06-12,10:00,15.21,15.21,15.2,15.2,1693,6
# hence split by comma can mistaken 14 fields as 15 fields or more
#       e.g. #split_col = functions.split( datafrm['value'], ',')
#	input:
# "GB0059822006","DLG","DIALOG SEMICOND.   LS-,10","Common stock","EUR",2505387,2018-06-12,10:00,15.21,15.21,15.2,15.2,1693,6
#	output:
# "GB0059822006","DLG","DIALOG SEMICOND.   LS-_10","Common stock","EUR",2505387,2018-06-12,10:00,15.21,15.21,15.2,15.2,1693,6
#
# ###current input line=["US5951121038","MTE","MICRON TECHN. INC. DL-,10","Common stock","EUR",2506531,2018-06-12,10:00,51.72,51.72,51.62,51.62,519,7]
#   @@@@@@@@@cleaned list=["US5951121038""MTE""MICRON TECHN. INC. DL-_10""Common stock""EUR"25065312018-06-1210:0051.7251.7251.6251.625197]
#
#  https://docs.python.org/2/library/tokenize.html
#	tokenize.generate_tokens(readline)
#The generate_tokens() generator requires one argument, readline, which must be a callable object which provides the same interface as the readline() method of built-in file objects (see section File Objects). Each call to the function should return one line of input as a string. Alternately, readline may be a callable object that signals completion by raising StopIteration.
# The generator produces 5-tuples with these members: the token type; the token string; a 2-tuple (srow, scol) of ints specifying the row and column where the token begins in the source; a 2-tuple (erow, ecol) of ints specifying the row and column where the token ends in the source; and the line on which the token was found. 
#
"""
def parts( inputString ):
    """Split a python-tokenizable expression on comma operators"""
    compos = [-1] # compos stores the positions of the relevant commas in the argument string
    compos.extend(tokens[2][1] for tokens in generate_tokens(StringIO( inputString ).readline) if tokens[1] == ',')
    compos.append(len( inputString ))
    #correctly spliting the inputString   using commas outside of quotes, but ignore within-quotes-comma
    return [  inputString[compos[i]+1:compos[i+1]] for i in xrange(len(compos)-1)]
    #return the correctly-split inputString using commas outside of quotes, but ignore within-quotes-comma

"""
Producer reads from S3 bucket 
Clean up all Pricing data
Send all Pricing data to Topic
@CONFIGURABLE: from_date  until_date   eachTimeSlot
"""
class Producer(object):
	def __init__(self, addr):
		self.producer = KafkaProducer(bootstrap_servers=addr)

if __name__ == "__main__":

	args = sys.argv
	# master node ip-10-0-0-9.ec2.internal
	ip_addr = "ec2-34-224-164-5.compute-1.amazonaws.com"
        if (len(args) > 2):
                ip_addr= str(args[1])

	producer = KafkaProducer(bootstrap_servers = 'ec2-34-224-164-5.compute-1.amazonaws.com')
	aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
	aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

	bucket_name ="germanymkt"
	conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
	bucket = conn.get_bucket(bucket_name)

	# initializations of path-names (year month day) and corresponding folders
	yrMDay = "2018-06-11"
	folder_name =yrMDay

        from_date = '2018-06-22'
        until_date= '2018-06-22'
        rangeDatesList = list(pd.date_range(from_date, until_date, freq='D').strftime('%Y-%m-%d'))
        print ("rangeDatesList ="+ str(rangeDatesList ) )

	# ACTIVE  Hours from 7 am to 3pm ; TESTING ***10am to 11am
	#s3a://deutsche-boerse-xetra-pds/2018-06-15/2018-06-15_BINS_XETR10.csv
	for yrMDay in rangeDatesList:

		folder_name =yrMDay
		#for eachTimeSlot in range(8,20): 
		# testing 
		for eachTimeSlot in range(11, 12): 
			file_name = yrMDay+ "_BINS_XETR"+str(eachTimeSlot)+".csv"
			if ( eachTimeSlot  < 10 ):
				file_name = yrMDay+ "_BINS_XETR0"+str( eachTimeSlot )+".csv"
			keyname = folder_name +"/"+ file_name

			##writing S3 csv file into temporary log out file
			if ( bucket.get_key( keyname ) == None):
				print ("###@@@ SKIPPING this loop iteration:  cannot get bucket keyname =" + keyname)
				continue
			elif (  bucket.get_key( keyname ) != None):
				# validate the type of bucket.get_key object
				tmpOutFile= "Out_"+  file_name
				bucket.get_key( keyname ).get_contents_to_filename (tmpOutFile)
				## split the S3 csv files into the allLines

			data = bucket.get_key( keyname).get_contents_as_string()
			allLines = data.split('\n')

			# validate the META-data of ach line
			for line in allLines:
				# parse input line into list of comma-separated & Quote-enclosed elements, but NOT split at within-quote-comma
				#print("### raw input line=["+ line + "]")
				cleanList = parts(line)

				# replace all Quote-enclosed commas with  Quote-enclosed underscore
				cleanList = [elt.replace(',', '_') for elt in cleanList]

				# stock ID ISIN is the key or first field before comma
				curLineKey =  cleanList[0] 
				cleanLineStr = ','.join( cleanList )

				if ( len(curLineKey)>=1 and curLineKey != "ISIN"):
					#print( "***Sending to GoodInTopic key=["+ curLineKey + "] and text={"+ cleanLineStr.encode('utf-8') + "}")
					#print( "***Sending to GoodInTopic text=["+ cleanLineStr.encode('utf-8') + "]")
					producer.send ( "GoodInTopic", value=cleanLineStr.encode('utf-8') )
					#producer.send ( "GoodInTopic", value=cleanLineStr.encode('utf-8'), key=curLineKey )
			## END for line in all Lines
			producer.flush()
			producer= KafkaProducer(retries =3)
		## END hour loops 
	## END DAY loops 
## fixed fields containing within-quotes-commas
