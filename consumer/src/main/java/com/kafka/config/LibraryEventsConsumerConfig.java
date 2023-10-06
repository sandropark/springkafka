package com.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

//@EnableKafka  // 최신 버전 스프링부트는 이 에너테이션을 달지 않아도 된다.
@Slf4j
@Configuration
public class LibraryEventsConsumerConfig {

    public DefaultErrorHandler errorHandler() {
        var fixedBackOff = new FixedBackOff(1000L, 2); // 1초 간격으로 2번 더 시도한다. (총 3번)

        var expBackOff = new ExponentialBackOffWithMaxRetries(2);  // 재시도 간격을 다르게 설정할 수 있다.
        expBackOff.setInitialInterval(1_000L);                                 // 초기 간격 : 1초
        expBackOff.setMultiplier(2.0);                                         // 간격 2배씩 증가. 1초 -> 2초 -> 4초
        expBackOff.setMaxInterval(2_000L);                                     // 최대 간격 : 2초

        var errorHandler = new DefaultErrorHandler(expBackOff);

        // Retry 하지 않을 예외 설정
//        List<Class<? extends Exception>> notRetryableExceptions = List.of(IllegalArgumentException.class);
//        notRetryableExceptions.forEach(errorHandler::addNotRetryableExceptions);

        // Retry 할 예외 설정
        var exceptionToRetryList = List.of(RecoverableDataAccessException.class);
        exceptionToRetryList.forEach(errorHandler::addRetryableExceptions);


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
