import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, to_date, datediff, current_date

def create_spark_connection():
    """
    Create a Spark session with Cassandra connector.
    """
    try:
        spark_conn = SparkSession.builder \
            .appName('GitHubBatchProcessing') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to: {e}")
        return None

def retrieve_top_languages(spark_conn):
    """
    Retrieve the top programming languages from the repositories.
    """
    try:
        df = spark_conn.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="repositories", keyspace="github_streams") \
            .load()

        # Group by language and count repositories
        top_languages = df.groupBy("language") \
            .agg(count("*").alias("repo_count")) \
            .orderBy(desc("repo_count")) \
            .limit(10)

        # Show the results
        top_languages.show()
    except Exception as e:
        logging.error(f"Could not retrieve top languages due to: {e}")

def analyze_trends(spark_conn):
    """
    Analyze trends in newly created repositories.
    """
    try:
        df = spark_conn.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="repositories", keyspace="github_streams") \
            .load()

        # Convert created_at to date and filter repositories created in the last 30 days
        df_filtered = df.withColumn("created_date", to_date(col("created_at"))) \
            .filter(datediff(current_date(), col("created_date")) <= 30)

        # Group by language and count repositories
        trends = df_filtered.groupBy("language") \
            .agg(count("*").alias("repo_count")) \
            .orderBy(desc("repo_count"))

        trends.show()
    except Exception as e:
        logging.error(f"Could not analyze trends due to: {e}")

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Retrieve top languages
        retrieve_top_languages(spark_conn)
        # Analyze trends in newly created repositories
        analyze_trends(spark_conn)