package com.kafka.entity;

import jakarta.persistence.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class LibraryEvent {
    @Id @GeneratedValue
    private Long libraryEventId;
    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;
    @ToString.Exclude
    @OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL)
    private Book book;

    public void map() {
        book.setLibraryEvent(this);
    }

    public void update(LibraryEvent libraryEvent) {
        libraryEventType = libraryEvent.getLibraryEventType();
        book.update(libraryEvent.getBook());
    }
}
