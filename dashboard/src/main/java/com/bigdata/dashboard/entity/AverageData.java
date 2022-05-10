package com.bigdata.dashboard.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.io.Serializable;
import java.util.Date;

@Table("averagedata")
public class AverageData implements Serializable {

	@PrimaryKeyColumn(name = "id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
	private String id;

	@Column(value = "temperature")
	private double temperature;

	@Column(value = "humidity")
	private double humidity;

	public String getId() {
		return id;
	}

	public double getTemperature() {
		return temperature;
	}

	public double getHumidity() {
		return humidity;
	}


	@Override
	public String toString() {
		return "AverageData [id=" + id + ", temperature=" + temperature + ", humidity=" + humidity  +" ]";
	}

}
