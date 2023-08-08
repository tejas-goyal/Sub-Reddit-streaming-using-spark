import praw
from kafka import KafkaProducer
import json
import time

# Set up Reddit API credentials
reddit = praw.Reddit(client_id='Xhm4uV7R-SQBWfd5jXlmkQ',
                     client_secret='jnVVQeeqJhNC4gOqDpg2hmDrrOCM2A',
                     redirect_uri='http://localhost:8080',
                     user_agent='myBot/0.0.1')



producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

subreddit = reddit.subreddit('all')
comments = subreddit.stream.comments(skip_existing=True)

for comment in comments:
    producer.send('reddit-comments', value=comment.body)
    print(comment.body)
    time.sleep(1)

