# Realtime-Streaming-Pipeline-with-kafka-Spark

This project demonstrates a data pipeline that fetches user data from an external API, streams it to Apache Kafka, processes it with Apache Spark, and provides orchestration with Apache Airflow.

### **Technologies Used**:
- **Apache Kafka**: For message streaming and buffering between producers and consumers.
- **Apache Spark**: For processing and transforming the streamed data.
- **Apache Airflow**: For orchestrating and managing the data pipeline tasks.
- **Docker**: To containerize and deploy all services.

### **Project Overview**:

1. **Data Source**: The pipeline fetches random user data from the [RandomUser API](https://randomuser.me/api/).
2. **Producer**: A Kafka producer sends the data to a Kafka topic called `user-data`.
3. **Kafka**: Kafka acts as a buffer between data ingestion and processing. Data is streamed to Kafka for real-time consumption.
4. **Spark**: Apache Spark processes the data from Kafka, transforming it as necessary.
5. **Orchestration**: Apache Airflow is used to manage the execution of the entire pipeline, ensuring the tasks run in the correct order and retries on failure.


## 🔧 In Progress

- Finalizing Kafka-Spark streaming integration
- Deciding on the output data sink (Cassandra or alternative)

