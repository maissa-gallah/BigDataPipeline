package com.bigdata.spark.util;

import com.bigdata.spark.entity.Temperature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class TemperatureDeserializer implements Deserializer<Temperature> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();

	public Temperature fromBytes(byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, Temperature.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public Temperature deserialize(String s, byte[] bytes) {
		return fromBytes((byte[]) bytes);
	}

	@Override
	public void close() {

	}
}
