'''from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'reddit-comments',
     bootstrap_servers=['localhost:9092'],
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    print(message.value)'''
    
import os
os.environ['SPARK_HOME'] = '/opt/spark'    
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count

spark = SparkSession.builder.appName('RedditComments').getOrCreate()

# Define the input stream from Kafka
df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'reddit-comments') \
    .option('startingOffsets', 'earliest') \
    .load()

# Parse the message value as JSON
df = df.selectExpr("CAST(value AS STRING)")

# Define a sliding window of 1 minute and count the number of comments in each window
windowedCounts = df.groupBy(window(df.timestamp, "1 minute")).agg(count('*').alias('comment_count'))

# Start the query and write the results to the console
query = windowedCounts.writeStream.outputMode('complete').format('console').start()

# Wait for the query to terminate
query.awaitTermination()


