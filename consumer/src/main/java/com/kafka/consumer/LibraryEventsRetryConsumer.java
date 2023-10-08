package com.kafka.consumer;

import com.kafka.service.LibraryEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class LibraryEventsRetryConsumer {

    private final LibraryEventService libraryEventService;

    @KafkaListener(topics = "${topics.retry}", groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> record) {
        log.info("record in Retry Consumer = {}", record);
        record.headers().forEach(header -> log.info("key={}, value={}", header.key(), new String(header.value())));
        libraryEventService.processLibraryEvent(record);
    }
}
