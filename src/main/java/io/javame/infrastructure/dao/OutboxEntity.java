package io.javame.infrastructure.dao;

import java.util.UUID;

/** Event pending broker confirmation. */
public record OutboxEntity(long id, UUID eventId, String symbol, String type,
                           String payload, int attempts) { }
