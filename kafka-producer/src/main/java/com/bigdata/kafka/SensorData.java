package com.bigdata.kafka;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class SensorData implements Serializable {

	private String id;
	private double temperature;
	private double humidity;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "MST")
	private Date timestamp;

	public SensorData() {

	}

	public SensorData(String id, double temperature, double humidity, Date timestamp) {
		super();
		this.id = id;
		this.temperature = temperature;
		this.humidity = humidity;
		this.timestamp = timestamp;
	}

	public String getId() {
		return id;
	}

	public double getTemperature() {
		return temperature;
	}

	public double getHumidity() {
		return humidity;
	}

	public Date getTimestamp() {
		return timestamp;
	}

}
