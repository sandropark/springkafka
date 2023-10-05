package intg.com.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.ConsumerApp;
import com.kafka.consumer.LibraryEventsConsumer;
import com.kafka.entity.Book;
import com.kafka.entity.LibraryEvent;
import com.kafka.entity.LibraryEventType;
import com.kafka.jpa.LibraryEventsRepository;
import com.kafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(topics = "library-events", partitions = 3)
@SpringBootTest(classes = ConsumerApp.class)
class LibraryEventsConsumerTest {

    @Autowired
    EmbeddedKafkaBroker broker;
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;
    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;
    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;
    @SpyBean
    LibraryEventService libraryEventServiceSpy;
    @Autowired
    LibraryEventsRepository libraryEventsRepository;
    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getAllListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, broker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws Exception {
        // Given
        String json = "{\"libraryEventId\":null,\"libraryEventType\": \"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        // When
        new CountDownLatch(1).await(3, TimeUnit.SECONDS);

        // Then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertThat(libraryEvents).hasSize(1);
        libraryEvents.forEach(libraryEvent -> {
            assertThat(libraryEvent.getLibraryEventId()).isNotNull();
            assertThat(libraryEvent.getBook().getBookId()).isEqualTo(456);
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws Exception {
        // Given
        String json = "{\"libraryEventId\":null,\"libraryEventType\": \"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.map();
        libraryEventsRepository.save(libraryEvent);

        // When
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        Book updateBook = Book.builder()
                .bookId(456L)
                .bookAuthor("Kafka Using Spring Boot 2.x")
                .bookName("Sandro")
                .libraryEvent(libraryEvent)
                .build();
        libraryEvent.setBook(updateBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(updatedJson).get();

//        new CountDownLatch(1).await(3, TimeUnit.SECONDS);
//
//        // Then
//        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
//        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
//
//        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
//        assertThat(libraryEvents).hasSize(1);
//        libraryEvents.forEach(libraryEvent -> {
//            assertThat(libraryEvent.getLibraryEventId()).isNotNull();
//            assertThat(libraryEvent.getBook().getBookId()).isEqualTo(456);
//        });
    }

}