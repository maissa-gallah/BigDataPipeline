package com.bigdata.spark.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.bigdata.spark.entity.Temperature;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.util.HashMap;
import java.util.Properties;

class ProcessorUtils {

	public static SparkConf getSparkConf(Properties prop) {
		var sparkConf = new SparkConf().setAppName(prop.getProperty("com.iot.app.spark.app.name"))
				.setMaster(prop.getProperty("com.iot.app.spark.master"))
				.set("spark.cassandra.connection.host", prop.getProperty("com.iot.app.cassandra.host"))
				.set("spark.cassandra.connection.port", prop.getProperty("com.iot.app.cassandra.port"))
				.set("spark.cassandra.auth.username", prop.getProperty("com.iot.app.cassandra.username"))
				.set("spark.cassandra.auth.password", prop.getProperty("com.iot.app.cassandra.password"))
				.set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("com.iot.app.cassandra.keep_alive"));

		if ("local".equals(prop.getProperty("com.iot.app.env"))) {
			sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
		}
		return sparkConf;
	}

	public static void saveDataToCassandra(final JavaDStream<Temperature> dataStream) {
		System.out.println("Saving to cassandra...");

		// Map Cassandra table column
		HashMap<String, String> columnNameMappings = new HashMap<>();
		columnNameMappings.put("id", "id");
		columnNameMappings.put("timestamp", "timestamp");
		columnNameMappings.put("value", "value");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(dataStream).writerBuilder("temperaturekeyspace", "temperature",
				CassandraJavaUtil.mapToRow(Temperature.class, columnNameMappings)).saveToCassandra();
	}

	public static void saveDataToHDFS(final JavaDStream<Temperature> dataStream, String saveFile, SparkConf conf) {
		System.out.println("Saving to hdfs...");

		SparkSession sql = SparkSession.builder().config(conf).getOrCreate();
		dataStream.foreachRDD(rdd -> {
			if (rdd.isEmpty()) {
				return;
			}
			Dataset<Row> dataFrame = sql.createDataFrame(rdd, Temperature.class);
			Dataset<Row> dfStore = dataFrame.selectExpr("id", "value", "timestamp");
			dfStore.printSchema();
			dfStore.write().mode(SaveMode.Append).json(saveFile);
		});
	}

}
