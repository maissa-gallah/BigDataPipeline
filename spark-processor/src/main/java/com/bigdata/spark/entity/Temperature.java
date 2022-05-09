package com.bigdata.spark.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonFormat;

public class Temperature implements Serializable{
	
	private String id;
	private double value;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
	private Date timestamp;
	private Map<String, String> metaData;
	
	public Temperature(){
		
	}

	public Temperature(String id, double value, Date timestamp) {
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
	
	public void setMetaData(Map<String, String> metaData) {
        this.metaData = metaData;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

}
