package io.javame.infrastructure.dao;

/** A durable, monotonically sequenced command inside a market. */
public record CommandEntity(String commandId, String symbol, long sequence, String kind,
                            String orderId, String userId, String status, String errorCode,
                            String message) { }
