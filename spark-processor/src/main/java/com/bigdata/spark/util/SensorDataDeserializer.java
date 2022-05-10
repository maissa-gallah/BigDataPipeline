package com.bigdata.spark.util;

import com.bigdata.spark.entity.SensorData;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class SensorDataDeserializer implements Deserializer<SensorData> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();

	public SensorData fromBytes(byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, SensorData.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public SensorData deserialize(String s, byte[] bytes) {
		return fromBytes((byte[]) bytes);
	}

	@Override
	public void close() {

	}
}
