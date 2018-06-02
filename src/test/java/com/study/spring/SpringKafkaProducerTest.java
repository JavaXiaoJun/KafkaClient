package com.study.spring;

import com.study.spring.kafka.KafkaProducerTemplate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.UnsupportedEncodingException;
import java.util.Random;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = KafkaSpringServer.class)
public class SpringKafkaProducerTest {

    @Autowired
    KafkaProducerTemplate producer;

    @Test
    public void test() throws UnsupportedEncodingException {
        Random random = new Random();
        while(true){
            int num = random.nextInt(20)%(20-0+1) + 0;
            String key = "15-WWW-80" + num +"|USA|1003";
            byte[] value = String.valueOf(num).getBytes("UTF-8");
            String topic = "kafka10test";
            ProducerRecord<byte[], byte[]> record = new ProducerRecord(topic,key.getBytes("UTF-8"),value);
            producer.send(record);
            System.out.println("**********" + num + "***********");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
