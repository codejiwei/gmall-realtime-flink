package com.codejiwei.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ClassName MyKafkaUtil
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/1/31 21:06
 * @Version 1.0
 **/
public class MyKafkaUtil {
    private static String bootstrap = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupID){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }


}
