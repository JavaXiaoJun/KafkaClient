package com.study.async.entity.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutorService;

/**
 * Created by lf52 on 2018/11/23.
 */
public class KafkaTaskHandler implements TaskHandler<KafkaConsumer<byte[], byte[]>> {

    @Override
    public void executeAsync(KafkaConsumer<byte[], byte[]> kafkaConsumer, ExecutorService asyncThreadPool) {
        while (true) {
            ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<byte[], byte[]> record : records)
                asyncThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            System.out.printf(Thread.currentThread().getName() + " -- async --> offset = %d, key = %s, value = %s , partition = %s%n", record.offset(), new String(record.key(), "UTF-8"), new String(record.value(), "UTF-8"), record.partition());
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                });

        }

    }

    @Override
    public void executeSync(KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        while (true) {
            ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<byte[], byte[]> record : records)
                try {
                    System.out.printf("sync --> offset = %d, key = %s, value = %s , partition = %s%n", record.offset(), new String(record.key(), "UTF-8"), new String(record.value(), "UTF-8"), record.partition());
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
        }

    }

}
