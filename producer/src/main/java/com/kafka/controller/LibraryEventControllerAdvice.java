package com.kafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.stream.Collectors;

@Slf4j
@ControllerAdvice
public class LibraryEventControllerAdvice {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<?> handleEx(MethodArgumentNotValidException ex) {
        var errorMessage = ex.getBindingResult().getFieldErrors().stream()
                .map(fieldError -> fieldError.getField() + " - " + fieldError.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining(", "));
        log.info("errorMessage = {}", errorMessage);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorMessage);
    }
}
