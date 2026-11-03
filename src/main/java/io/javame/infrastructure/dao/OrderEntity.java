package io.javame.infrastructure.dao;

import java.math.BigDecimal;

/** Immutable persisted order snapshot; pending orders are excluded from book queries. */
public record OrderEntity(String orderId, String userId, String symbol, String side,
                          BigDecimal price, BigDecimal quantity, BigDecimal remaining,
                          String status, long priority) { }
