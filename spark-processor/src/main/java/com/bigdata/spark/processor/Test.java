package com.bigdata.spark.processor;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import com.bigdata.spark.entity.Temperature;
import com.bigdata.spark.util.TemperatureDeserializer;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.bigdata.spark.util.PropertyFileReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Test {

	public static void main(String[] args) throws Exception {

		String file = "spark-processor-local.properties";
		Properties prop = PropertyFileReader.readPropertyFile(file);

		SparkConf conf = ProcessorUtils.getSparkConf(prop);

		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
		streamingContext.checkpoint(prop.getProperty("com.iot.app.spark.checkpoint.dir"));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("com.iot.app.kafka.brokerlist"));
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TemperatureDeserializer.class);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty("com.iot.app.kafka.topic"));
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop.getProperty("com.iot.app.kafka.resetType"));
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		Collection<String> topics = Arrays.asList("temperature-data-event");

		JavaInputDStream<ConsumerRecord<String, Temperature>> stream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, Temperature> Subscribe(topics, kafkaParams));

		JavaDStream<Temperature> dataStream = stream.map(v -> {
			return v.value();
		});

		dataStream.print();
		
		// save data to cassandra => stream
		//ProcessorUtils.saveDataToCassandra(dataStream);
		
		// save data to HDFS => batch
		String saveFile = prop.getProperty("com.iot.app.hdfs") + "iot-data";
		ProcessorUtils.saveDataToHDFS( dataStream,  saveFile, conf);

		streamingContext.start();
		streamingContext.awaitTermination();

	}

}
