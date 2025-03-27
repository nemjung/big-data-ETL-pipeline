## Data Streaming ETL Pipeline with Kafka and Cassandra (Dockerized)

This project demonstrates the development of a real-time data streaming ETL (Extract, Transform, Load) pipeline using Apache Kafka and Cassandra, containerized with Docker. It serves as a practical exercise in building scalable and resilient data architectures.

The pipeline consists of the following key stages:

* **Data Production & Kafka Integration:** A Python producer reads and transforms a large CSV dataset (170,000+ rows), publishing the processed data as a continuous stream to an Apache Kafka topic.
* **Real-time Data Consumption & Batch Processing:** A Python consumer subscribes to the Kafka topic, retrieves the streaming data, and processes it in batches of 5,000 records before loading.
* **Scalable Data Storage with Cassandra:** The processed data is efficiently ingested into a Cassandra NoSQL database for persistent storage and scalable querying.
* **Full Containerization with Docker:** The entire infrastructure, including the data producer, Kafka broker, Kafka consumer, and Cassandra database, is containerized using Docker and Docker Compose for easy setup, portability, and reproducibility.

**Key Technologies Used:**

* Python (`kafka-python`)
* Apache Kafka
* Cassandra
* Docker
* Docker Compose
* **PySpark**

This project provides a hands-on understanding of building real-time data pipelines, integrating with distributed messaging systems and NoSQL databases, and leveraging containerization for deployment and management. Future enhancements may explore advanced data transformation and processing with PySpark.
