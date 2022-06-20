# Big Data Pipeline for monitoring Temperature and Humidity using Lambda Architecture

The application is an IoT data processing and monitoring application. 

We divided the application into three modules. These modules are standalone Maven applications written in Java and can be built and run independently.

1. **Kafka Producer:** This module simulates Realtime IoT data coming from a Sensor. It generates mock Temperature and Humidity values every 2 to 5s which are then sent as events to a topic in Kafka.

2. **Spark Processor:** This module contains two Processors :
    - **Stream Processor** : This part of the Speed Layer ( view architecture ). Here, we are processing the streaming data using Kafka with Spark Streaming API. The SensorData is processed By Spark Straming which will seperates it into **Temperature** and **Humidity** and storing it in Cassandra. Simultaneously, the streamed data is then appended into Hadoop HDFS for batch processing.
    - **Batch Processor** : The processor needs visibility on all the SensorData in order to calculate the average of both Temperature and Humidity. We are processing the batch data using Spark and storing the pre-computed views into Cassandra.

3. **Dashboard:** This is a Spring Boot application which will retrieve data from the Cassandra database and send it to a web page. This application uses Web Sockets and jQuery to push the streamed data to the web page in fixed intervals so data will be refreshed automatically. The average values are pushed in different intervals since Batch Processing takes a longer time. We used two different types of Google chart tools to showcase realtime and average values.

### Architecture

We used **Lambda architecture** which is a data-processing architecture designed to handle massive quantities of data by taking advantage of both batch and stream-processing methods.

![architecture](https://raw.githubusercontent.com/ShathaCodes/BigData/main/arch.PNG](https://github.com/maissa-gallah/BigDataPipeline/blob/main/arch.PNG)


### Stack

- Java 1.8
- Maven
- SpringBoot
- ZooKeeper
- Kafka
- Cassandra
- Spark 
- Hadoop HDFS
- Docker

### Execution

All component parts are dynamically managed using Docker, which means you don't need to worry about setting up your local environment, the only thing you need is to have Docker installed. Just run 

```
docker-compose up
```

You must also create the database schema in Cassandra

```
docker exec -it cassandra-iot cqlsh --username cassandra --password cassandra -f /schema.cql
```

You must also create the folder to save the data for later batch processing
```
docker exec namenode hdfs dfs -rm -r /lambda-arch
docker exec namenode hdfs dfs -mkdir /lambda-arch
docker exec namenode hdfs dfs -mkdir /lambda-arch/checkpoint
docker exec namenode hdfs dfs -chmod -R 777 /lambda-arch
docker exec namenode hdfs dfs -chmod -R 777 /lambda-arch/checkpoint
```

Run the Stream Processor
```
docker exec spark-master /spark/bin/spark-submit --class com.bigdata.spark.processor.StreamProcessor --master spark://localhost:7077 /opt/spark-data/bigdata-spark-processor-1.0.0.jar
```

Run the Batch Processor
```
docker exec spark-master /spark/bin/spark-submit --class com.bigdata.spark.processor.BatchProcessor --master spark://localhost:7077 /opt/spark-data/bigdata-spark-processor-1.0.0.jar
```

You can view the dashboard on
```
localhost:3000
```
