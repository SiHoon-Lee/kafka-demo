package com.kafka.demo.process;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.LocalDateTime;

@Slf4j
@Component
public class KafkaProcess {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Scheduled(fixedRate = 100)
    public void testTopicSender(){

        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send("my-topic", "Time : " + LocalDateTime.now());
        future.addCallback(
                new ListenableFutureCallback<SendResult<Integer, String>>() {

                    @Override
                    public void onSuccess(SendResult<Integer, String> result) {
                        log.info("sent message='{}' with offset={}", result.getProducerRecord().value(), result.getRecordMetadata().offset());
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        log.error("unable to send message ", ex);
                    }
                }
        );
    }

    @KafkaListener(topics = "my-topic", groupId = "my-topic-app")
    public void listen(String message) {
        log.info("Received Message : {}", message);
    }

}
