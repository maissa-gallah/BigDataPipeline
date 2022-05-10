package com.bigdata.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SensorDataProducer {

	private final Producer<String, SensorData> producer;

	public SensorDataProducer(final Producer<String, SensorData> producer) {
		this.producer = producer;
	}

	public static void main(String[] args) throws Exception {
		Properties properties = PropertyFileReader.readPropertyFile();
		Producer<String, SensorData> producer = new Producer<>(new ProducerConfig(properties));
		SensorDataProducer iotProducer = new SensorDataProducer(producer);
		iotProducer.generateIoTEvent(properties.getProperty("kafka.topic"));
	}

	private void generateIoTEvent(String topic) throws InterruptedException {
		Random rand = new Random();
		double init_val_temp = 20;
		double init_val_hum = 80;
		System.out.println("Sending events");

		while (true) {
			SensorData event = generateSensorData(rand, init_val_temp, init_val_hum);
			producer.send(new KeyedMessage<>(topic, event));
			Thread.sleep(rand.nextInt(5000 - 2000) + 2000); // random delay of 2 to 5 seconds
		}
	}

	private SensorData generateSensorData(final Random rand, double temp, double hum) {
		String id = UUID.randomUUID().toString();
		Date timestamp = new Date();
		double t = temp + rand.nextDouble() * 10;
		double h = hum + rand.nextDouble() * 10;

		SensorData data = new SensorData(id, t, h, timestamp);
		return data;
	}
}
