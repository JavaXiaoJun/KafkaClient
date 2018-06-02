package com.study.spring.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class KafkaProducerTemplate<K,V> {

    @Autowired
    private KafkaTemplate<K, V> template;

    public ListenableFuture<SendResult<K, V>> send(ProducerRecord<K, V> record){
        return this.template.send(record);
    }


}
