package com.sandro.springkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sandro.springkafka.domain.LibraryEvent;
import com.sandro.springkafka.util.TestUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestPropertySource(properties = {
        // 설정 오버라이드
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(
        topics = "library-events"
)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LibraryEventControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        var configs = KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(),
                new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void post() throws Exception {
        // Given
        var httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        var requestEntity = new HttpEntity<>(TestUtil.libraryEventRecord(), httpHeaders);

        // When
        var responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, requestEntity, LibraryEvent.class);

        // Then
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        var records = KafkaTestUtils.getRecords(consumer);
        assertThat(records.count()).isEqualTo(1);
        records.forEach(record -> {
            var libraryEvent = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            log.info("libraryEvent = {}", libraryEvent);
            assertThat(libraryEvent).isEqualTo(TestUtil.libraryEventRecord());
        });
    }

}