package com.codejiwei.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ClassName MyKafkaUtilBak
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/2/3 20:45
 * @Version 1.0
 **/
public class MyKafkaUtilBak {
    private static String KAFKA_BOOTSTRAP_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final  String KAFKA_DEFAULT_TOPIC = "default_topic";

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupID) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }


    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 6 * 1000 + "");
        return new FlinkKafkaProducer<T>(KAFKA_DEFAULT_TOPIC, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
