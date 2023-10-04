package com.sandro.springkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sandro.springkafka.domain.LibraryEvent;
import com.sandro.springkafka.producer.LibraryEventsProducer;
import com.sandro.springkafka.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    ObjectMapper objectMapper;
    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {
        // Given
        String requestBody = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());
        when(libraryEventsProducer.send(isA(LibraryEvent.class)))
                .thenReturn(null);

        // When
        ResultActions resultActions = mvc.perform(post("/v1/libraryevent")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
        );

        // Then
        resultActions.andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent_4xx() throws Exception {
        // Given
        String requestBody = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());
        when(libraryEventsProducer.send(isA(LibraryEvent.class)))
                .thenReturn(null);

        // When
        ResultActions resultActions = mvc.perform(post("/v1/libraryevent")
                .content(requestBody)
                .contentType(MediaType.APPLICATION_JSON)
        );

        // Then
        resultActions
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("book.bookId - must not be null, book.bookName - must not be empty"));
    }

}