import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from github import Github
import json
from kafka import KafkaProducer
import time
import logging
import os
from dotenv import load_dotenv

load_dotenv()
# GitHub API credentials
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
KAFKA_BROKER = "broker:29092"

# Initialize GitHub API client
github = Github(GITHUB_TOKEN)

default_args = {
    'owner': 'Lina',
    'start_date': datetime(2023, 9, 3, 10, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_github_data():
    """
    Fetch the latest GitHub repositories with high stars.
    """
    days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    query = f"stars:>1000 created:>{days_ago}"
    repos = github.search_repositories(query, sort="stars", order="desc")[:10]
    return [repo for repo in repos]

def format_github_data(repo):
    """
    Format GitHub repository data for Kafka.
    """
    data = {
        "id": repo.id,
        "name": repo.full_name,
        "stars": repo.stargazers_count,
        "forks": repo.forks_count,
        "language": repo.language,
        "created_at": repo.created_at.isoformat(),
        "updated_at": repo.updated_at.isoformat(),
        "url": repo.html_url,
    }
    return data

def stream_github_data():
    """
    Stream GitHub repository data to Kafka.
    """
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            repos = get_github_data()
            for repo in repos:
                formatted_data = format_github_data(repo)
                producer.send('github_repos', json.dumps(formatted_data).encode('utf-8'))
                logging.info(f"Published to Kafka: {formatted_data}")
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

# Define the DAG
with DAG('github_automation',
         default_args=default_args,
         schedule_interval='@daily',  # Run daily
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_github_data',
        python_callable=stream_github_data
    )
