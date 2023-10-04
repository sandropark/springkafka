package com.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
//@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    @KafkaListener(
            topics = "library-events"
    )
    @Override
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment ack) {
        log.info("ConsumerRecord = {}", consumerRecord);
        ack.acknowledge();
    }

}
