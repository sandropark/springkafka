package com.kafka.config;

import com.kafka.service.FailureService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

//@EnableKafka  // 최신 버전 스프링부트는 이 에너테이션을 달지 않아도 된다.
@Slf4j
@RequiredArgsConstructor
@Configuration
public class LibraryEventsConsumerConfig {
    private static final String DEAD = "DEAD";
    private static final String RETRY = "RETRY";

    @Value("${topics.retry}")
    private String retryTopic;
    @Value("${topics.dlt}")
    private String deadLetterTopic;
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final FailureService failureService;

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (consumerRecord, e) -> {
                    log.error("Record in DeadLetterPublishingRecoverer = {}", consumerRecord);
                    String topic = deadLetterTopic;
                    if (e.getCause() instanceof RecoverableDataAccessException)
                        topic = retryTopic;
                    return new TopicPartition(topic, consumerRecord.partition());
                });
    }

    private ConsumerRecordRecoverer consumerRecordRecoverer() {
        return (consumerRecord, e) -> {
            var record = (ConsumerRecord<Integer, String>) consumerRecord;
            log.error("Record in PublishingRecoverer = {}", consumerRecord);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                log.info("inside Recovery");
                failureService.save(record, e, RETRY);
            } else {
                log.info("inside Non-Recovery");
                failureService.save(record, e, DEAD);
            }
        };
    }

    public DefaultErrorHandler errorHandler() {
        var fixedBackOff = new FixedBackOff(1000L, 2); // 1초 간격으로 2번 더 시도한다. (총 3번)

        var expBackOff = new ExponentialBackOffWithMaxRetries(2);  // 재시도 간격을 다르게 설정할 수 있다.
        expBackOff.setInitialInterval(1_000L);                                 // 초기 간격 : 1초
        expBackOff.setMultiplier(2.0);                                         // 간격 2배씩 증가. 1초 -> 2초 -> 4초
        expBackOff.setMaxInterval(2_000L);                                     // 최대 간격 : 2초

        var errorHandler = new DefaultErrorHandler(
                consumerRecordRecoverer(),
//                publishingRecoverer(),
                expBackOff);

        // Retry 하지 않을 예외 설정
        List<Class<? extends Exception>> notRetryableExceptions = List.of(IllegalArgumentException.class);
        notRetryableExceptions.forEach(errorHandler::addNotRetryableExceptions);

        // Retry 할 예외 설정
//        var exceptionToRetryList = List.of(RecoverableDataAccessException.class);
//        exceptionToRetryList.forEach(errorHandler::addRetryableExceptions);


        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.info("Failed Record in Retry Listener, Exception = {}, deliveryAttempt = {}", ex.getMessage(), deliveryAttempt)
        );
        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable());
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
