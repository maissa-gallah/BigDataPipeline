package com.bigdata.spark.entity;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class Humidity implements Serializable {

	private String id;
	private double value;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "MST")
	private Date timestamp;

	public Humidity() {

	}

	public Humidity(String id, double value, Date timestamp) {
		super();
		this.id = id;
		this.value = value;
		this.timestamp = timestamp;
	}

	public String getId() {
		return id;
	}

	public double getValue() {
		return value;
	}

	public Date getTimestamp() {
		return timestamp;
	}

}
