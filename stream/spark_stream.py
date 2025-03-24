import os
os.environ['CASS_DRIVER_NO_CYTHON'] = '1'
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from cassandra.cluster import Cluster


# Define the schema for the incoming JSON data
def define_schema():
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("stars", IntegerType(), False),
        StructField("forks", IntegerType(), False),
        StructField("language", StringType(), True),
        StructField("created_at", StringType(), False),
        StructField("updated_at", StringType(), False),
        StructField("url", StringType(), False),
    ])

# Initialize Spark Session
def initialize_spark_session():
    return SparkSession.builder \
        .appName("KafkaStructuredStreaming") \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config('spark.cassandra.connection.host', 'cassandra') \
        .getOrCreate()

# Read streaming data from Kafka
def read_from_kafka(spark, kafka_broker, topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

def parse_kafka_data(df, schema):
    return (df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*") \
        .filter(col("language").isNotNull() & (col("language") != "Unknown") & (col("language") != "null")))


def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        session.execute("SELECT release_version FROM system.local")
        logging.info("Connected to Cassandra successfully!")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS github_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
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

def write_to_console(parsed_df):
    return parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

def write_to_cassandra(parsed_df):
    return parsed_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "github_streams") \
        .option("table", "repositories") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .outputMode("append") \
        .start()

if __name__ == "__main__":

    kafka_broker = "broker:9092"
    topic = "github_repos"

    schema = define_schema()

    spark = initialize_spark_session()

    kafka_df = read_from_kafka(spark, kafka_broker, topic)

    parsed_df = parse_kafka_data(kafka_df, schema)

    session = create_cassandra_connection()
    create_keyspace(session)
    create_table(session)

    query = write_to_cassandra(parsed_df)

    query.awaitTermination()