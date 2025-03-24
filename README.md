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

# Airflow dashboard
   
<img width="1268" alt="Screenshot 2025-03-24 at 10 36 51 AM" src="https://github.com/user-attachments/assets/afde7a0c-8d8d-4ff6-8ae7-9f8bf93f0a77" />

# Kafka data

<img width="1280" alt="Screenshot 2025-03-17 at 12 52 44 AM" src="https://github.com/user-attachments/assets/65e97d9a-4196-4771-9275-0e42f1bf075b" />

# Cassandra database

<img width="1198" alt="Screenshot 2025-03-24 at 1 34 47 AM" src="https://github.com/user-attachments/assets/38c765c4-7a2a-4afd-b84b-9dc72c13afb5" />

## How to Run

1. **Clone the repository**  
```
git clone https://github.com/luluna02/Github-Data-Pipeline
cd Github-Data-Pipeline
```
2. **Start Containers**
```
docker compose up -d
```
3. **Run Spark Job**
```
docker exec -it src-spark-master-1 spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --master spark://172.19.0.4:7077 \
  spark_batch.py
```
Make sure to replace the spark master url with the correct IP address.
