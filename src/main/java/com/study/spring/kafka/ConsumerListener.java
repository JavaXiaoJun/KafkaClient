package com.study.spring.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

import java.io.UnsupportedEncodingException;


public class ConsumerListener {


    /**
     * 消费1或者多个topic
     * @param record
     */
    //@KafkaListener(id = "test1",topics = {"${kafka.topic}","${kafka.topic1}"})
    @KafkaListener(id = "test1",topics = {"${kafka.topic}"})
    public void listen(ConsumerRecord<byte[], byte[]> record)  {
        try {
            System.err.printf("Consumer Test1 : offset = %d, key = %s, value = %s , partition = %s%n", record.offset(), new String(record.key(), "UTF-8"), new String(record.value(), "UTF-8"), record.partition());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }


    /**
     * 配置topic和分区：监听topic1下分区0，3的消息
     * @param record
     */
    @KafkaListener(id = "test2", topicPartitions = { @TopicPartition(topic = "${kafka.topic}", partitions = { "${kafka.partition1}", "${kafka.partition2}"})})
    public void listen1(ConsumerRecord<byte[], byte[]> record)  {
        try {
            System.err.printf("Consumer Test2 : offset = %d, key = %s, value = %s , partition = %s%n", record.offset(), new String(record.key(), "UTF-8"), new String(record.value(), "UTF-8"), record.partition());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * 配置topic和分区和开始offset：监听topic下分区partition1，offset开始的消息
     * @param record
     */
    @KafkaListener(id = "test3", topicPartitions = { @TopicPartition(topic = "${kafka.topic}", partitionOffsets = @PartitionOffset(partition = "${kafka.partition1}", initialOffset = "${kafka.offset}"))})
    public void listen2(ConsumerRecord<byte[], byte[]> record)  {
        try {
            System.err.printf("Consumer Test3 : offset = %d, key = %s, value = %s , partition = %s%n", record.offset(), new String(record.key(), "UTF-8"), new String(record.value(), "UTF-8"), record.partition());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

}
