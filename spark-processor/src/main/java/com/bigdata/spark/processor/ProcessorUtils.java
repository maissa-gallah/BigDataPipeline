package com.bigdata.spark.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.bigdata.spark.entity.AverageData;
import com.bigdata.spark.entity.Humidity;
import com.bigdata.spark.entity.SensorData;
import com.bigdata.spark.entity.Temperature;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

	public static void saveTemperatureToCassandra(final JavaDStream<Temperature> dataStream) {
		System.out.println("Saving to cassandra...");

		// Map Cassandra table column
		HashMap<String, String> columnNameMappings = new HashMap<>();
		columnNameMappings.put("id", "id");
		columnNameMappings.put("timestamp", "timestamp");
		columnNameMappings.put("value", "value");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(dataStream).writerBuilder("sensordatakeyspace", "temperature",
				CassandraJavaUtil.mapToRow(Temperature.class, columnNameMappings)).saveToCassandra();
	}

	public static void saveHumidityToCassandra(final JavaDStream<Humidity> dataStream) {
		System.out.println("Saving to cassandra...");

		// Map Cassandra table column
		HashMap<String, String> columnNameMappings = new HashMap<>();
		columnNameMappings.put("id", "id");
		columnNameMappings.put("timestamp", "timestamp");
		columnNameMappings.put("value", "value");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(dataStream).writerBuilder("sensordatakeyspace", "humidity",
				CassandraJavaUtil.mapToRow(Humidity.class, columnNameMappings)).saveToCassandra();
	}

	public static void saveAvgToCassandra(JavaRDD<AverageData> rdd) {
		CassandraJavaUtil.javaFunctions(rdd)
				.writerBuilder("sensordatakeyspace", "averagedata", CassandraJavaUtil.mapToRow(AverageData.class))
				.saveToCassandra();

	}

	public static void saveDataToHDFS(final JavaDStream<SensorData> dataStream, String saveFile, SparkSession sql) {
		System.out.println("Saving to hdfs...");

		dataStream.foreachRDD(rdd -> {
			if (rdd.isEmpty()) {
				return;
			}
			Dataset<Row> dataFrame = sql.createDataFrame(rdd, SensorData.class);

			Dataset<Row> dfStore = dataFrame.selectExpr("id", "temperature", "humidity", "timestamp");
			dfStore.printSchema();
			dfStore.write().mode(SaveMode.Append).parquet(saveFile);
		});
	}

	public static SensorData transformData(Row row) {
		System.out.println(row);
		return new SensorData(row.getString(0), row.getDouble(1), row.getDouble(2), new Date(2022, 5, 5));
	}

	public static List<AverageData> runBatch(SparkSession sparkSession, String saveFile) {
		System.out.println("Running Batch Processing");
		var dataFrame = sparkSession.read().parquet(saveFile);
		System.out.println(dataFrame);
		JavaRDD<SensorData> rdd = dataFrame.javaRDD().map(row -> ProcessorUtils.transformData(row));

		JavaRDD<Double> temp = rdd.map(data -> {
			return data.getTemperature();
		});

		JavaRDD<Double> hum = rdd.map(data -> {
			return data.getHumidity();
		});

		double avg_temp = temp.reduce((value1, value2) -> value1 + value2);

		double avg_hum = hum.reduce((value1, value2) -> value1 + value2);

		long length = temp.count();

		avg_temp /= length;

		avg_hum /= length;

		System.out.println("Avg temp : " + avg_temp);

		System.out.println("Avg hum : " + avg_hum);

		AverageData d = new AverageData("0", avg_temp, avg_hum);
		List<AverageData> average_data_list = new ArrayList<AverageData>();
		average_data_list.add(d);

		return average_data_list;
	}

}
