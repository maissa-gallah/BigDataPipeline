package com.bigdata.spark.entity;

import java.io.Serializable;

public class AverageData implements Serializable {

	private String id;
	private double temperature;

	private double humidity;

	public AverageData() {

	}

	public AverageData(String id, double temperature, double humidity) {
		super();
		this.id = id;
		this.temperature = temperature;
		this.humidity = humidity;
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

}
