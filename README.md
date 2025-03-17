## The pipeline is built to:

1. **Ingest Data**  
   Fetch data from the GitHub API using **Apache Airflow** and stream it using **Apache Kafka**.

2. **Process Data**  
   Perform real-time processing and analysis of the data using **Apache Spark** for streaming.

3. **Store Data**  
   Store the processed data in **Apache Cassandra** for efficient querying and retrieval.

4. **Containerize the Pipeline**  
   Use **Docker** to containerize the entire pipeline, ensuring easy deployment and scalability.
