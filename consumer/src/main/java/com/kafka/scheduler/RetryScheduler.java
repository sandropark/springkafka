package com.kafka.scheduler;

import com.kafka.entity.FailureRecord;
import com.kafka.jpa.FailureRecordRepository;
import com.kafka.service.LibraryEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static com.kafka.entity.FailureRecordType.RETRY;
import static com.kafka.entity.FailureRecordType.SUCCESS;

@Slf4j
@RequiredArgsConstructor
@Component
public class RetryScheduler {

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventService libraryEventService;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("Retrying Failed Records Started!");
        failureRecordRepository.findAllByStatus(RETRY)
                .forEach(failureRecord -> {
                    log.info("Retrying Failed Record = {}", failureRecord);
                    var consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(SUCCESS);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecord = {}", e.getMessage(), e);
                    }
                });
        log.info("Retrying Failed Records Completed!");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord record) {
        return new ConsumerRecord<>(
                record.getTopic(),
                record.getPartition(),
                record.getOffsetValue(),
                record.getKeyValue(),
                record.getErrorRecord()
        );
    }
}