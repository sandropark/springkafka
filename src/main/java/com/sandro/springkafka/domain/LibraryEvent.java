package com.sandro.springkafka.domain;

import java.awt.print.Book;

public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEventType,
        Book book
) {

}
