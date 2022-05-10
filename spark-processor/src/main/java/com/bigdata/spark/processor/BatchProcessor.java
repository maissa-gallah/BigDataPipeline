package com.bigdata.spark.processor;


import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.bigdata.spark.entity.AverageData;
import com.bigdata.spark.util.PropertyFileReader;

public class BatchProcessor {
	
	public static void main(String[] args) throws Exception {

		String file = "spark-processor-local.properties";
		Properties prop = PropertyFileReader.readPropertyFile(file);
		SparkConf conf = ProcessorUtils.getSparkConf(prop);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        
		String saveFile = prop.getProperty("com.iot.app.hdfs") + "iot-data";

		List<AverageData> average_data_list = ProcessorUtils.runBatch(sparkSession, saveFile);
		
		JavaRDD<AverageData> h = sc.parallelize(average_data_list, 1); // transform to RDD
		ProcessorUtils.saveAvgToCassandra(h);

		sparkSession.close();
		sparkSession.stop();

	}

}
