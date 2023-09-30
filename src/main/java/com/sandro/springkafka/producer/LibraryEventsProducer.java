package com.sandro.springkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sandro.springkafka.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
@Component
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public CompletableFuture<SendResult<Integer, String>> send(LibraryEvent libraryEvent) {
        var key = libraryEvent.libraryEventId();
        var value = writeValueAsString(libraryEvent);
        var completableFuture = kafkaTemplate.send(topic, key, value);
        return completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) handleFailure(key, value, throwable);
                    else handleSuccess(key, value, sendResult);
                });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent Successfully for the key : {} and the value : {} partition is : {}",
                key,
                value,
                sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending the message and the exception is {}", ex.getMessage(), ex);
    }

    private String writeValueAsString(LibraryEvent libraryEvent) {
        try {
            return objectMapper.writeValueAsString(libraryEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
