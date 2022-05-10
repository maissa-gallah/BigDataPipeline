package com.bigdata.dashboard.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.bigdata.dashboard.entity.AverageData;
import com.bigdata.dashboard.repository.AverageDataRepository;

@Service
public class AverageService {

	@Autowired
	private SimpMessagingTemplate template;

	@Autowired
	private AverageDataRepository averageDataRepository;

	// Method sends data message in every 60 seconds.
	@Scheduled(fixedRate = 15000)
	public void trigger() {

		Long time = new Date().getTime();
		Date date = new Date(time - time % ( 60 * 1000)); // get data from the last minute

		AverageData data = averageDataRepository.find();
		System.out.println(data);

		double temperature = data.getTemperature();
		System.out.println(temperature);

		double humidity = data.getHumidity();
		System.out.println(humidity);
		// prepare response
		Response response = new Response();
		response.setHumidity(humidity);
		response.setTemperature(temperature);

		// send to ui
		this.template.convertAndSend("/topic/average", response);
	}

}
