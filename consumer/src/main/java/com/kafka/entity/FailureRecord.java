package com.kafka.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class FailureRecord {
    @Id @GeneratedValue
    private Long bookId;
    private String topic;
    private Integer keyValue;
    private Integer partition;
    private String errorRecord;
    private Long offsetValue;
    private String exception;
    private FailureRecordType status;
}
