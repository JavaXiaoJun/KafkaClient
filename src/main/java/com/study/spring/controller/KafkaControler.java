package com.study.spring.controller;

import com.study.spring.kafka.KafkaProducerTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.UnsupportedEncodingException;

@Controller
@EnableAutoConfiguration
@PropertySource("classpath:schedule.properties")
public class KafkaControler {

    @Autowired
    KafkaProducerTemplate producer;

    @RequestMapping("/send")
    @ResponseBody
    String send(String key, String value)  {
        String topic = "kafka10test";
        ProducerRecord<byte[], byte[]> record = null;
        try {
            record = new ProducerRecord(topic,key.getBytes("UTF-8"),value.getBytes("UTF-8"));
            System.out.println("Producer : " + record);
            producer.send(record);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "0";
    }

    @RequestMapping("/sendToPartiton")
    @ResponseBody
    String sendToPartiton(String key, String value , int partiton) {
        String topic = "kafka10test";
        ProducerRecord<byte[], byte[]> record = null;
        try {
            record = new ProducerRecord(topic,partiton,key.getBytes("UTF-8"),value.getBytes("UTF-8"));
            System.out.println("Producer : " + record);
            producer.send(record);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "0";
    }

}
