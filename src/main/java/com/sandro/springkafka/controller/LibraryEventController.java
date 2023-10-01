package com.sandro.springkafka.controller;

import com.sandro.springkafka.domain.LibraryEvent;
import com.sandro.springkafka.domain.LibraryEventType;
import com.sandro.springkafka.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequiredArgsConstructor
@RestController
public class LibraryEventController {

    private final LibraryEventsProducer libraryEventsProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> create(@RequestBody @Valid LibraryEvent libraryEvent) {
        log.info("libraryEvent = {}", libraryEvent);
//        libraryEventsProducer.send(libraryEvent);
//        libraryEventsProducer.send_producerRecord(libraryEvent);
        libraryEventsProducer.send_sync(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> update(@RequestBody @Valid LibraryEvent libraryEvent) {
        log.info("libraryEvent = {}", libraryEvent);

        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if(BAD_REQUEST != null) return BAD_REQUEST;

        libraryEventsProducer.send_sync(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }

    private ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibarayEventId");

        if (!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE))
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        return null;
    }
}
