package com.kafka.jpa;

import com.kafka.entity.FailureRecord;
import com.kafka.entity.FailureRecordType;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Long> {
    List<FailureRecord> findAllByStatus(FailureRecordType status);
}
