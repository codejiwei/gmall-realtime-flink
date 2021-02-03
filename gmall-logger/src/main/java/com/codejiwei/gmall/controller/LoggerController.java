package com.codejiwei.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Properties;

/**
 * @ClassName LoggerController
 * @Description TODO
 * @Author codejiwei
 * @Date 2021/1/29 14:54
 * @Version 1.0
 **/
@RestController
@Slf4j
public class LoggerController {

    //KafkaTemplate是Spring提供对Kafka操作的类
    @Autowired  //注入
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String jsonLog){
        //1 打印输出到控制台
//        System.out.println(jsonLog);

        //2 落盘 借助记录日志的第三方框架log4j 【logback】
        log.info(jsonLog);

        //3 将生成的日志发送到kafka对应的主题中
        //TODO 方式1：使用kafkaProducer
//        Properties config = new Properties();
//        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
//        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(config);
//
//        kafkaProducer.send(new ProducerRecord<String, String>("ods_base_log", jsonLog));
        //TODO 方式2：使用Spring提供的KafkaTemplate
        kafkaTemplate.send("ods_base_log", jsonLog);

        return "success";
    }
}
