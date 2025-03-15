
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import praw  # Reddit API library
import json
from kafka import KafkaProducer
import time
import logging

reddit = praw.Reddit(
    client_id="Y2ti-Pk00ZGsHzHRnEjBwlw",
    client_secret="_wlnPiQDdsdtm5oDng18HzhmVLzZ_g",
    user_agent="python:reddit_analytics_pipeline:v1.0",
)

default_args = {
    'owner': 'Lina',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def get_reddit_data():
    """
    Fetch real-time Reddit posts from the 'all' subreddit.
    """
    subreddit = reddit.subreddit("all")  # Monitor all subreddits
    posts = []
    for post in subreddit.stream.submissions():
        posts.append(post)
        if len(posts) >= 10:  # Fetch 10 posts for each batch
            break
    return posts

def format_reddit_data(post):
    """
    Format Reddit post data for Kafka.
    """
    data = {
        "id": post.id,
        "title": post.title,
        "subreddit": post.subreddit.display_name,
        "author": post.author.name if post.author else "Unknown",
        "upvotes": post.score,
        "comments": post.num_comments,
        "created_utc": post.created_utc,
        "url": post.url,
    }
    return data

def stream_reddit_data():
    """
    Stream Reddit posts to Kafka.
    """
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Stream for 1 minute
            break
        try:
            posts = get_reddit_data()
            for post in posts:
                formatted_data = format_reddit_data(post)
                producer.send('reddit_posts', json.dumps(formatted_data).encode('utf-8'))
                logging.info(f"Published to Kafka: {formatted_data}")
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

# Define the DAG
with DAG('reddit_analytics',
         default_args=default_args,
         schedule_interval='@daily',  # Run daily
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_reddit_data',
        python_callable=stream_reddit_data
    )