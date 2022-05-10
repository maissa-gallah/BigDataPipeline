package com.bigdata.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class SensorDataEncoder implements Encoder<SensorData> {
		
	private static ObjectMapper objectMapper = new ObjectMapper();		
	public SensorDataEncoder(VerifiableProperties verifiableProperties) {

    }
	public byte[] toBytes(SensorData event) {
		try {
			String msg = objectMapper.writeValueAsString(event);
			System.out.println(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			System.out.println("Error in Serialization" +e.getMessage());
		}
		return null;
	}
}
