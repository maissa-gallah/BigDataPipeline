package com.bigdata.dashboard.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

import com.bigdata.dashboard.entity.Temperature;

import java.util.Date;
import java.util.UUID;

@Repository
public interface TemperatureRepository extends CassandraRepository<Temperature, UUID>{

	 @Query("SELECT * FROM sensordatakeyspace.temperature WHERE timestamp > ?0 ALLOW FILTERING")
	 Iterable<Temperature> findTemperatureByDate(Date date);
}
