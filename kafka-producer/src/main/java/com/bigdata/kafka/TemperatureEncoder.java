package com.bigdata.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class TemperatureEncoder implements Encoder<Temperature> {
		
	private static ObjectMapper objectMapper = new ObjectMapper();		
	public TemperatureEncoder(VerifiableProperties verifiableProperties) {

    }
	public byte[] toBytes(Temperature event) {
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
