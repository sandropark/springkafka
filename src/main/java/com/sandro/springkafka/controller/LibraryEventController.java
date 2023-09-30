package com.sandro.springkafka.controller;

import com.sandro.springkafka.domain.LibraryEvent;
import com.sandro.springkafka.producer.LibraryEventsProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequiredArgsConstructor
@RestController
public class LibraryEventController {

    private final LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
        log.info("libraryEvent = {}", libraryEvent);
//        libraryEventsProducer.send(libraryEvent);
        libraryEventsProducer.send_producerRecord(libraryEvent);
        log.info("After Sending libraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
