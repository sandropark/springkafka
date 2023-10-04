package com.sandro.springkafka.domain;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public record Book(
        @NotNull
        Integer bookId,
        @NotEmpty
        String bookName,
        @NotEmpty
        String bookAuthor
) {
}
