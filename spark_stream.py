import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define the schema for GitHub repository data
github_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("stars", IntegerType(), False),
    StructField("forks", IntegerType(), False),
    StructField("language", StringType(), True),
    StructField("created_at", StringType(), False),
    StructField("updated_at", StringType(), False),
    StructField("url", StringType(), False),
])

def create_keyspace(session):
    """
    Create a keyspace in Cassandra for storing GitHub repository data.
    """
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS github_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    """
    Create a table in Cassandra for storing GitHub repository data.
    """
    session.execute("""
        CREATE TABLE IF NOT EXISTS github_streams.repositories (
            id INT PRIMARY KEY,
            name TEXT,
            stars INT,
            forks INT,
            language TEXT,
            created_at TEXT,
            updated_at TEXT,
            url TEXT
        );
    """)
    logging.info("Table created successfully!")

def insert_data(session, **kwargs):
    """
    Insert GitHub repository data into Cassandra.
    """
    try:
        session.execute("""
            INSERT INTO github_streams.repositories (id, name, stars, forks, language, created_at, updated_at, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            kwargs.get('id'),
            kwargs.get('name'),
            kwargs.get('stars'),
            kwargs.get('forks'),
            kwargs.get('language'),
            kwargs.get('created_at'),
            kwargs.get('updated_at'),
            kwargs.get('url')
        ))
        logging.info(f"Data inserted for repository: {kwargs.get('name')}")
    except Exception as e:
        logging.error(f"Could not insert data due to: {e}")

def create_spark_connection():
    """
    Create a Spark session with Cassandra and Kafka connectors.
    """
    try:
        spark_conn = SparkSession.builder \
            .appName('GitHubDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to: {e}")
        return None

def connect_to_kafka(spark_conn):
    """
    Connect to Kafka and create a streaming DataFrame.
    """
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'github_repos') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created due to: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    """
    Parse the Kafka DataFrame and filter out repositories without a language.
    """
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), github_schema).alias('data')) \
        .select("data.*") \
        .filter(col("language").isNotNull() & (col("language") != "Unknown"))  # Filter out null/unknown languages
    return sel

def create_cassandra_connection():
    """
    Create a connection to Cassandra.
    """
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None

if __name__ == "__main__":

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            # Parse and filter the Kafka DataFrame
            selection_df = create_selection_df_from_kafka(spark_df)

            # Create Cassandra connection
            session = create_cassandra_connection()
            if session is not None:
                # Create keyspace and table
                create_keyspace(session)
                create_table(session)

                logging.info("Streaming is being started...")

                # Write the streaming data to Cassandra
                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'github_streams')
                                   .option('table', 'repositories')
                                   .start())

                streaming_query.awaitTermination()