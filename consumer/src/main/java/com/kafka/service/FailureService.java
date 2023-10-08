package com.kafka.service;

import com.kafka.entity.FailureRecord;
import com.kafka.entity.FailureRecordType;
import com.kafka.jpa.FailureRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class FailureService {
    private final FailureRecordRepository failureRecordRepository;

    public void save(ConsumerRecord<Integer, String> consumerRecord, Exception e, FailureRecordType status) {
        var failureRecord  = FailureRecord.builder()
                .status(status)
                .topic(consumerRecord.topic())
                .keyValue(consumerRecord.key())
                .errorRecord(consumerRecord.value())
                .offsetValue(consumerRecord.offset())
                .partition(consumerRecord.partition())
                .exception(e.getCause().getMessage())
                .build();
        failureRecordRepository.save(failureRecord);
    }
}
