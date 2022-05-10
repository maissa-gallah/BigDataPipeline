package com.bigdata.spark.processor;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import com.bigdata.spark.entity.Humidity;
import com.bigdata.spark.entity.SensorData;
import com.bigdata.spark.entity.Temperature;
import com.bigdata.spark.util.SensorDataDeserializer;


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
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorDataDeserializer.class);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty("com.iot.app.kafka.topic"));
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop.getProperty("com.iot.app.kafka.resetType"));
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		Collection<String> topics = Arrays.asList(prop.getProperty("com.iot.app.kafka.topic"));

		JavaInputDStream<ConsumerRecord<String, SensorData>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, SensorData>Subscribe(topics, kafkaParams));

		JavaDStream<SensorData> sensordataStream = stream.map(v -> {
			return v.value();
		});

		sensordataStream.print();
		
		JavaDStream<Temperature> temperatureStream = sensordataStream.map(v -> {
			return new Temperature(v.getId(),v.getTemperature(),v.getTimestamp());
		});
		temperatureStream.print();
		
		JavaDStream<Humidity> humidityStream = sensordataStream.map(v -> {
			return new Humidity(v.getId(),v.getHumidity(),v.getTimestamp());
		});

		// save data to cassandra => stream
		 ProcessorUtils.saveTemperatureToCassandra(temperatureStream);
		 
		 ProcessorUtils.saveHumidityToCassandra(humidityStream);

		/*
		// save data to HDFS => batch
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		String saveFile = prop.getProperty("com.iot.app.hdfs") + "iot-data";
		ProcessorUtils.saveDataToHDFS(sensordataStream, saveFile, sparkSession);

		// todo batch process
		var dataFrame = sparkSession.read().json(saveFile);
		JavaRDD<Temperature> rdd = dataFrame.javaRDD().map(row -> ProcessorUtils.transformData(row));

		sparkSession.close();
		sparkSession.stop();
		*/
		
		streamingContext.start();
		streamingContext.awaitTermination();

	}

}
