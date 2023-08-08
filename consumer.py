from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import mysql.connector

# Set up Spark session
spark = SparkSession.builder.appName("reddit-streaming-consumer").getOrCreate()

# Set up StreamingContext with batch interval of 5 seconds
ssc = StreamingContext(spark.sparkContext, 5)

# Set up Kafka parameters
kafkaParams = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "reddit-group",
    "auto.offset.reset": "largest"
}

# Set up list of topics to subscribe to
topics = ["reddit_stream"]

# Define schema for incoming JSON data
jsonSchema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", IntegerType(), True),
    StructField("subreddit", StringType(), True),
    StructField("over_18", BooleanType(), True)
])

# Create function to save data to MySQL database
'''def save_to_mysql(rdd):
    # Create MySQL connection
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="reddit"
    )
    mycursor = mydb.cursor()

    # Loop through RDD and insert data into MySQL table
    for row in rdd.collect():
        sql = "INSERT INTO reddit_posts (id, title, score, num_comments, created_utc, subreddit, over_18) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (row["id"], row["title"], row["score"], row["num_comments"], row["created_utc"], row["subreddit"], row["over_18"])
        mycursor.execute(sql, val)

    # Commit changes and close connection
    mydb.commit()
    mycursor.close()'''

# Create Kafka stream
stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)

# Parse incoming JSON data and select relevant columns
parsed_stream = stream.map(lambda x: x[1]).\
    select(from_json(col("value").cast("string"), jsonSchema).alias("json")).\
    select("json.*")

# Count number of posts within each window
posts_count = parsed_stream.count()

# Group posts by subreddit and count number of posts within each subreddit in each window
subreddit_counts = parsed_stream.groupBy("subreddit").count()

# Min and max scores of posts within each window
min_max_scores = parsed_stream.selectExpr("min(score) as min_score", "max(score) as max_score")

# Print results to console
posts_count.pprint()
subreddit_counts.pprint()
min_max_scores.pprint()

# Save some data to MySQL database
#parsed_stream.foreachRDD(lambda rdd: save_to_mysql(rdd))

# Start the streaming context
ssc.start()
ssc.awaitTermination()
