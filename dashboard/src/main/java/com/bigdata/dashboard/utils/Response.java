package com.bigdata.dashboard.utils;

import java.io.Serializable;
import java.util.List;



public class Response implements Serializable {
    private double temperature;
    private double humidity;

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

   
}
