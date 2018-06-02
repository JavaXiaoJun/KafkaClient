package com.study.spring;

import com.study.spring.kafka.ConsumerListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages={"com.study.spring"})
public class KafkaSpringServer {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringServer.class, args);
    }

    @Bean//消息监听器
    public ConsumerListener myListener() {
        return new ConsumerListener();
    }


}
