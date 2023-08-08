import json
import praw
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Reddit API credentials
reddit = praw.Reddit(client_id='Xhm4uV7R-SQBWfd5jXlmkQ',
                     client_secret='jnVVQeeqJhNC4gOqDpg2hmDrrOCM2A',
                     user_agent='reddit streaming data analysis',
                     username='tejas_goyal',
                     password='6303@Anriya')

# Kafka Producer configuration
producer_server = "localhost:9092"
producer_topic = "reddit_stream"

# Function to extract required data from Reddit stream and send it to Kafka topic
def send_to_kafka():
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=producer_server,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    for comment in reddit.subreddit('all').stream.comments():
        producer.send(producer_topic, {'title': comment.submission.title, 
                                       'subreddit': comment.submission.subreddit.display_name, 
                                       'score': comment.submission.score})
        producer.flush()

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("RedditStream") \
    .getOrCreate()

# Define schema of json
schema = StructType() \
    .add("title", StringType()) \
    .add("subreddit", StringType()) \
    .add("score", IntegerType())

# Read data from Kafka
kafka_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", producer_server) \
  .option("subscribe", producer_topic) \
  .load() \
  .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

# Process and save data
query = kafka_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Start the Spark Streaming Context and the Kafka Producer
query.awaitTermination()
send_to_kafka()
