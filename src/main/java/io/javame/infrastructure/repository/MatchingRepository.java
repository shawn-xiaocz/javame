package io.javame.infrastructure.repository;

import io.javame.infrastructure.dao.CommandEntity;
import io.javame.infrastructure.dao.OrderEntity;
import io.javame.infrastructure.dao.OutboxEntity;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Storage boundary. Lock methods must execute inside a database transaction. */
public interface MatchingRepository {
    void registerMarket(String symbol, int priceScale, int volumeScale);
    void lockMarket(String symbol);
    long nextSequence(String symbol);
    boolean insertOrder(OrderEntity order);
    Optional<OrderEntity> getOrder(String oid);
    Optional<CommandEntity> getCommand(String commandId);
    void insertCommand(CommandEntity command);
    Optional<CommandEntity> lockNextCommand();
    Optional<OrderEntity> getBestMaker(OrderEntity taker);
    void updateOrder(String oid, BigDecimal remaining, String status);
    void completeCommand(String commandId, String status, String errorCode, String message);
    void insertTrade(UUID tradeId, String symbol, String makerId, String takerId,
                     BigDecimal price, BigDecimal volume, Instant time);
    void insertEvent(UUID eventId, String symbol, String type, String payload);
    List<DepthLevel> listDepth(String symbol, String side, int offset, int count);
    long countDepth(String symbol, String side);
    Optional<OutboxEntity> lockNextEvent();
    void markPublished(long id);
    void deferEvent(long id, int delaySeconds);

    record DepthLevel(BigDecimal price, BigDecimal volume) { }
}
