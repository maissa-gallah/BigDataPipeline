package com.bigdata.dashboard.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import com.bigdata.dashboard.repository.TemperatureRepository;
import com.bigdata.dashboard.repository.HumidityRepository;

/**
 * Service class to send data messages to dashboard ui at fixed interval using
 * web-socket.
 */
@Service
public class DataService {

	@Autowired
	private SimpMessagingTemplate template;

	@Autowired
	private TemperatureRepository temperatureRepository;

	@Autowired
	private HumidityRepository humidityRepository;

	// Method sends data message in every 10 seconds.
	@Scheduled(fixedRate = 10000)
	public void trigger() {
		System.out.println("triggered");
		List<Double> temperatures = new ArrayList<>();
		List<Double> humidities = new ArrayList<>();

		Long time = new Date().getTime();
		Date date = new Date(time - time % ( 60 * 1000)); // get data from the last minute
		//Date date = new Date(time - time % (2 * 24 * 60 * 60 * 1000));

		temperatureRepository.findTemperatureByDate(date).forEach(e -> temperatures.add(e.getValue()));
		humidityRepository.findHumidityByDate(date).forEach(e -> humidities.add(e.getValue()));

		// temperatureRepository.findTemperatureByDate(date).forEach(e ->
		// temperatures.add(e.getValue()));
		// humidityRepository.findHumidityByDate(date).forEach(e ->
		// humidities.add(e.getValue()));

		double temperature = temperatures.size() > 0 ? temperatures.get(temperatures.size() - 1) : 20;
		double humidity = humidities.size() > 0 ? humidities.get(humidities.size() - 1) : 80;

		// prepare response
		Response response = new Response();
		response.setHumidity(humidity);
		response.setTemperature(temperature);

		// send to ui
		this.template.convertAndSend("/topic/data", response);
	}

}
