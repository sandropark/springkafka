package com.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.LibraryEvent;
import com.kafka.jpa.LibraryEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
@Service
public class LibraryEventService {

    private final LibraryEventsRepository libraryEventsRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void processLibraryEvent(ConsumerRecord<Integer, String> record) {
        LibraryEvent libraryEvent = readValue(record);
        log.info("LibraryEvent = {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> {
                libraryEvent.map();
                libraryEventsRepository.save(libraryEvent);
                log.info("Successfully Persisted the Library Event = {}", libraryEvent);
            }
            case UPDATE -> {
                validate(libraryEvent);
                LibraryEvent foundLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
                        .orElseThrow(() -> new IllegalArgumentException("Not a valid library Event"));
                foundLibraryEvent.update(libraryEvent);
            }
            default -> log.error("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) throw new IllegalArgumentException("Library Event Id is missing");
        log.info("Validation is successful for the library Event = {}", libraryEvent);
    }

    private LibraryEvent readValue(ConsumerRecord<Integer, String> record) {
        try {
            return objectMapper.readValue(record.value(), LibraryEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
