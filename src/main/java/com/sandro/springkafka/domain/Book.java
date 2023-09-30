package com.sandro.springkafka.domain;

public record Book(
        Integer bookId,
        String bookName,
        String bookAuthor
) {
}
