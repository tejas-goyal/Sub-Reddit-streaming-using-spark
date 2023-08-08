import praw
import json
import time
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import mysql.connector

# Reddit API credentials
reddit = praw.Reddit(client_id='Xhm4uV7R-SQBWfd5jXlmkQ',
                     client_secret='jnVVQeeqJhNC4gOqDpg2hmDrrOCM2A',
                     user_agent='reddit streaming data analysis',
                     username='tejas_goyal',
                     password='6303@Anriya')

# Kafka Producer configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

# Kafka Consumer configuration
kafkaParams = {"metadata.broker.list": "localhost:9092"}
topics = ['reddit_stream']

# Spark Streaming configuration
ssc = StreamingContext(SparkSession.builder.appName("RedditStream").getOrCreate(), 10)
kafkaStream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)

# Function to process and save data to MariaDB
def process_data(rdd):
    if not rdd.isEmpty():
        # Convert RDD to DataFrame
        spark = SparkSession.builder.appName("RedditStream").getOrCreate()
        df = spark.read.json(rdd)

        '''# Create connection to MariaDB
        cnx = mysql.connector.connect(user='<db_use', password='<db_password>',
                                       host='<db_host>', database='<db_name>')
        cursor = cnx.cursor()

        # Insert data into the movies table
        for row in df.rdd.collect():
            sql = "INSERT INTO movies (title, subreddit, score) VALUES (%s, %s, %s)"
            val = (row['title'], row['subreddit'], row['score'])
            cursor.execute(sql, val)
        cnx.commit()

        # Close MariaDB connection
        cursor.close()
        cnx.close()'''

# Function to extract required data from Reddit stream and send it to Kafka topic
def send_to_kafka():
    for comment in reddit.subreddit('all').stream.comments():
        producer.send('reddit_stream', {'title': comment.submission.title, 
                                        'subreddit': comment.submission.subreddit.display_name, 
                                        'score': comment.submission.score})

# Start the Spark Streaming Context
kafkaStream.foreachRDD(process_data)
ssc.start()

# Start the Kafka Producer
send_to_kafka()
