package com.bigdata.spark.processor;

import com.bigdata.spark.entity.Temperature;
import com.bigdata.spark.util.TemperatureDeserializer;
import com.bigdata.spark.util.PropertyFileReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import scala.reflect.ClassTag;

public class StreamingProcessor implements Serializable {

    private static final Logger logger = Logger.getLogger(StreamingProcessor.class);
    private final Properties prop;

    public StreamingProcessor(Properties properties) {
        this.prop = properties;
    }

    public static void main(String[] args) throws Exception {
    	System.out.println("start");
        String file = "iot-spark-local.properties";
//        String file = "iot-spark.properties";
        Properties prop = PropertyFileReader.readPropertyFile(file);
        StreamingProcessor streamingProcessor = new StreamingProcessor(prop);
        streamingProcessor.start();
    }

    private void start() throws Exception {
    	System.out.println("start2");
        String parqueFile = prop.getProperty("com.iot.app.hdfs") + "iot-data-parque";
        Map<String, Object> kafkaProperties = getKafkaParams(prop);
        SparkConf conf = ProcessorUtils.getSparkConf(prop);
        System.out.println("start3");
        //batch interval of 10 seconds for incoming stream
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
        System.out.println("start4");
        streamingContext.checkpoint(prop.getProperty("com.iot.app.spark.checkpoint.dir"));
        System.out.println("start5");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        System.out.println("start6");
        Map<TopicPartition, Long> offsets = getOffsets(parqueFile, sparkSession);
        System.out.println("start7");
        JavaInputDStream<ConsumerRecord<String, Temperature>> kafkaStream = getKafkaStream(
                prop,
                streamingContext,
                kafkaProperties,
                offsets
        );
        System.out.println("start8");
        logger.info("Starting Stream Processing");

        kafkaStream.transform(StreamingProcessor::transformRecord);
        System.out.println("start9");
        commitOffset(kafkaStream);
        System.out.println("start10");
        streamingContext.start();
        streamingContext.awaitTermination();
    }
    
    private static JavaRDD<Temperature> transformRecord(JavaRDD<ConsumerRecord<String, Temperature>> item) {
        OffsetRange[] offsetRanges;
        offsetRanges = ((HasOffsetRanges) item.rdd()).offsetRanges();
        return item.mapPartitionsWithIndex(addMetaData(offsetRanges), true);
    }
    
    private static Function2<Integer, Iterator<ConsumerRecord<String, Temperature>>, Iterator<Temperature>> addMetaData(
            final OffsetRange[] offsetRanges
    ) {
        return (index, items) -> {
            List<Temperature> list = new ArrayList<>();
            while (items.hasNext()) {
                ConsumerRecord<String, Temperature> next = items.next();
                Temperature dataItem = next.value();
                System.out.println("tessssst = " + dataItem.getValue());

                list.add(dataItem);
            }
            return list.iterator();
        };
    }

    private Map<TopicPartition, Long> getOffsets(final String parqueFile, final SparkSession sparkSession) {
        try {
            LatestOffSetReader latestOffSetReader = new LatestOffSetReader(sparkSession, parqueFile);
            return latestOffSetReader.read().offsets();
        } catch (Exception e) {
            return new HashMap<>();
        }
    }


    /**
     * Commit the ack to kafka after process have completed
     *
     * @param kafkaStream
     */
    private void commitOffset(JavaInputDStream<ConsumerRecord<String, Temperature>> kafkaStream) {
        kafkaStream.foreachRDD((JavaRDD<ConsumerRecord<String, Temperature>> trafficRdd) -> {
            if (!trafficRdd.isEmpty()) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) trafficRdd.rdd()).offsetRanges();

                CanCommitOffsets canCommitOffsets = (CanCommitOffsets) kafkaStream.inputDStream();
                canCommitOffsets.commitAsync(offsetRanges, new TrafficOffsetCommitCallback());
            }
        });
    }


    private Map<String, Object> getKafkaParams(Properties prop) {
    	System.out.println(prop.getProperty("com.iot.app.kafka.topic"));
        Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("com.iot.app.kafka.brokerlist"));
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TemperatureDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty("com.iot.app.kafka.topic"));
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, prop.getProperty("com.iot.app.kafka.resetType"));
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return kafkaProperties;
    }


    private JavaInputDStream<ConsumerRecord<String, Temperature>> getKafkaStream(
            Properties prop,
            JavaStreamingContext streamingContext,
            Map<String, Object> kafkaProperties,
            Map<TopicPartition, Long> fromOffsets
    ) {
        List<String> topicSet = Arrays.asList(new String[]{prop.getProperty("com.iot.app.kafka.topic")});
        if (fromOffsets.isEmpty()) {
            return KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topicSet, kafkaProperties)
            );
        }

        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSet, kafkaProperties, fromOffsets)
        );
    }



}

final class TrafficOffsetCommitCallback implements OffsetCommitCallback, Serializable {

    private static final Logger log = Logger.getLogger(TrafficOffsetCommitCallback.class);

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        log.info("---------------------------------------------------");
        log.info(String.format("{0} | {1}", new Object[]{offsets, exception}));
        log.info("---------------------------------------------------");
    }
}
