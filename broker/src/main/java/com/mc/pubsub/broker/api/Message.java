package com.mc.pubsub.broker.api;

import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Сообщение. Создается топиком при пуше из строки body и помещается в очередь
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class Message {

    @EqualsAndHashCode.Include
    @ToString.Include
    private UUID id;

    private OffsetDateTime createdDate;
    private String body;
}
