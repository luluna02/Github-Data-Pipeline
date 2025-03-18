![Untitled Diagram drawio](https://github.com/user-attachments/assets/24a74f7b-719b-4efb-a5ea-4dc89b95e505)


## The pipeline is built to:

1. **Ingest Data**  
   Fetch data from the GitHub API using **Apache Airflow** and stream it using **Apache Kafka**.

2. **Process Data**  
   Perform real-time processing and analysis of the data using **Apache Spark** for streaming.
   Perform batch analysis of the data using **Apache Spark SQl** .

4. **Store Data**  
   Store the processed data in **Apache Cassandra** for efficient querying and retrieval.

5. **Containerize the Pipeline**  
   Use **Docker** to containerize the entire pipeline.
   

