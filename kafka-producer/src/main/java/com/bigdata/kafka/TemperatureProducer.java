package com.bigdata.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TemperatureProducer {

	private final Producer<String, Temperature> producer;

	public TemperatureProducer(final Producer<String, Temperature> producer) {
		this.producer = producer;
	}

	public static void main(String[] args) throws Exception {
		Properties properties = PropertyFileReader.readPropertyFile();
		Producer<String, Temperature> producer = new Producer<>(new ProducerConfig(properties));
		TemperatureProducer iotProducer = new TemperatureProducer(producer);
		iotProducer.generateIoTEvent(properties.getProperty("kafka.topic"));
	}

	private void generateIoTEvent(String topic) throws InterruptedException {
		Random rand = new Random();
		double init_val = 20;
		System.out.println("Sending events");

		while (true) {
			Temperature event = generateTemperature(rand , init_val );
			producer.send(new KeyedMessage<>(topic, event));
			Thread.sleep(rand.nextInt(5000 - 2000) + 2000); // random delay of 2 to 5 seconds
		}
	}

	private Temperature generateTemperature(final Random rand, double val) {
		String id = UUID.randomUUID().toString();
		Date timestamp = new Date();
		double value = val+ rand.nextDouble() * 10;
		
		Temperature temp = new Temperature(id, value, timestamp);
		return temp;
	}
}
