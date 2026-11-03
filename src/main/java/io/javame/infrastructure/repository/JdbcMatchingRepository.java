package io.javame.infrastructure.repository;

import io.javame.infrastructure.dao.*;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** MySQL InnoDB row locks coordinate all replicas without Redis leases or local locks. */
@Repository
public class JdbcMatchingRepository implements MatchingRepository {
    private static final String ORDER_COLUMNS = "order_id,user_id,symbol,side,price,quantity,remaining,status,priority";
    private static final String COMMAND_COLUMNS = "command_id,symbol,market_sequence,kind,order_id,user_id,status,error_code,message";
    private final JdbcTemplate jdbc;

    public JdbcMatchingRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void registerMarket(String symbol, int priceScale, int volumeScale) {
        jdbc.update("INSERT INTO market(symbol,price_scale,volume_scale) VALUES (?,?,?) ON DUPLICATE KEY UPDATE symbol=symbol",
                symbol, priceScale, volumeScale);
        Boolean matches = jdbc.queryForObject(
                "SELECT price_scale = ? AND volume_scale = ? FROM market WHERE symbol = ?",
                Boolean.class, priceScale, volumeScale, symbol);
        if (!Boolean.TRUE.equals(matches)) {
            throw new IllegalStateException("Market precision differs from the persisted configuration: " + symbol);
        }
    }

    @Override
    public void lockMarket(String symbol) {
        jdbc.queryForObject("SELECT symbol FROM market WHERE symbol = ? FOR UPDATE", String.class, symbol);
    }

    @Override
    public long nextSequence(String symbol) {
        // The caller holds this market row until commit; both statements use the same transaction.
        jdbc.update("UPDATE market SET command_sequence = command_sequence + 1 WHERE symbol = ?", symbol);
        return jdbc.queryForObject("SELECT command_sequence FROM market WHERE symbol = ?", Long.class, symbol);
    }

    @Override
    public boolean insertOrder(OrderEntity order) {
        try {
            return jdbc.update("INSERT INTO matching_order(" + ORDER_COLUMNS + ") VALUES (?,?,?,?,?,?,?,?,?)",
                    order.orderId(), order.userId(), order.symbol(), order.side(), order.price(), order.quantity(),
                    order.remaining(), order.status(), order.priority()) == 1;
        } catch (DuplicateKeyException ex) {
            // InnoDB rolls back the duplicate INSERT statement, not the surrounding transaction.
            // Do not use INSERT IGNORE: invalid values and constraint failures must remain errors.
            return false;
        }
    }

    @Override
    public Optional<OrderEntity> getOrder(String oid) {
        return jdbc.query("SELECT " + ORDER_COLUMNS + " FROM matching_order WHERE order_id = ?",
                JdbcMatchingRepository::order, oid).stream().findFirst();
    }

    @Override
    public Optional<CommandEntity> getCommand(String commandId) {
        return jdbc.query("SELECT " + COMMAND_COLUMNS + " FROM matching_command WHERE command_id = ?",
                JdbcMatchingRepository::command, commandId).stream().findFirst();
    }

    @Override
    public void insertCommand(CommandEntity command) {
        jdbc.update("INSERT INTO matching_command(command_id,symbol,market_sequence,kind,order_id,user_id) VALUES (?,?,?,?,?,?)",
                command.commandId(), command.symbol(), command.sequence(), command.kind(), command.orderId(), command.userId());
    }

    @Override
    public Optional<CommandEntity> lockNextCommand() {
        // Lock the market first everywhere: placement, cancellation, and workers share one order.
        List<String> symbols = jdbc.queryForList("""
                SELECT m.symbol FROM market m
                WHERE EXISTS (SELECT 1 FROM matching_command c WHERE c.symbol=m.symbol AND c.status='PENDING')
                ORDER BY m.last_processed_at,m.symbol LIMIT 1 FOR UPDATE SKIP LOCKED
                """, String.class);
        if (symbols.isEmpty()) {
            return Optional.empty();
        }
        String symbol = symbols.get(0);
        jdbc.update("UPDATE market SET last_processed_at=CURRENT_TIMESTAMP(6) WHERE symbol=?", symbol);
        return jdbc.query("SELECT " + COMMAND_COLUMNS + " FROM matching_command WHERE symbol=? AND status='PENDING' ORDER BY market_sequence LIMIT 1",
                JdbcMatchingRepository::command, symbol).stream().findFirst();
    }

    @Override
    public Optional<OrderEntity> getBestMaker(OrderEntity taker) {
        boolean buy = "BUY".equals(taker.side());
        // Only fixed SQL fragments are chosen; no caller-controlled text enters the query.
        String comparison = buy ? "<=" : ">=";
        String ordering = buy ? "ASC" : "DESC";
        String side = buy ? "SELL" : "BUY";
        return jdbc.query("SELECT " + ORDER_COLUMNS + " FROM matching_order WHERE symbol=? AND side=? "
                        + "AND status IN ('OPEN','PARTIALLY_FILLED') AND price " + comparison + " ? "
                        + "ORDER BY price " + ordering + ",priority ASC LIMIT 1 FOR UPDATE",
                JdbcMatchingRepository::order, taker.symbol(), side, taker.price()).stream().findFirst();
    }

    @Override
    public void updateOrder(String oid, BigDecimal remaining, String status) {
        jdbc.update("UPDATE matching_order SET remaining=?,status=?,updated_at=CURRENT_TIMESTAMP(6) WHERE order_id=?",
                remaining, status, oid);
    }

    @Override
    public void completeCommand(String commandId, String status, String errorCode, String message) {
        jdbc.update("UPDATE matching_command SET status=?,error_code=?,message=?,processed_at=CURRENT_TIMESTAMP(6) WHERE command_id=?",
                status, errorCode, message, commandId);
    }

    @Override
    public void insertTrade(UUID tradeId, String symbol, String makerId, String takerId,
                            BigDecimal price, BigDecimal volume, Instant time) {
        jdbc.update("INSERT INTO trade(trade_id,symbol,maker_order_id,taker_order_id,price,quantity,traded_at) VALUES (?,?,?,?,?,?,?)",
                tradeId.toString(), symbol, makerId, takerId, price, volume, Timestamp.from(time));
    }

    @Override
    public void insertEvent(UUID eventId, String symbol, String type, String payload) {
        jdbc.update("INSERT INTO event_outbox(event_id,symbol,event_type,payload) VALUES (?,?,?,?)",
                eventId.toString(), symbol, type, payload);
    }

    @Override
    public List<DepthLevel> listDepth(String symbol, String side, int offset, int count) {
        String ordering = "BUY".equals(side) ? "DESC" : "ASC";
        return jdbc.query("SELECT price,SUM(remaining) AS volume FROM matching_order WHERE symbol=? AND side=? "
                        + "AND status IN ('OPEN','PARTIALLY_FILLED') GROUP BY price ORDER BY price " + ordering + " LIMIT ? OFFSET ?",
                (rs, row) -> new DepthLevel(rs.getBigDecimal("price"), rs.getBigDecimal("volume")), symbol, side, count, offset);
    }

    @Override
    public long countDepth(String symbol, String side) {
        return jdbc.queryForObject("SELECT COUNT(DISTINCT price) FROM matching_order WHERE symbol=? AND side=? AND status IN ('OPEN','PARTIALLY_FILLED')",
                Long.class, symbol, side);
    }

    @Override
    public Optional<OutboxEntity> lockNextEvent() {
        // A delayed/locked head blocks subsequent events of that symbol, not other markets.
        return jdbc.query("""
                SELECT e.id,e.event_id,e.symbol,e.event_type,e.payload,e.attempts FROM event_outbox e
                WHERE e.published_at IS NULL AND e.available_at <= CURRENT_TIMESTAMP(6)
                AND NOT EXISTS (SELECT 1 FROM event_outbox p
                    WHERE p.symbol=e.symbol AND p.published_at IS NULL AND p.id<e.id)
                ORDER BY e.id LIMIT 1 FOR UPDATE SKIP LOCKED
                """, (rs, row) -> new OutboxEntity(rs.getLong("id"), UUID.fromString(rs.getString("event_id")),
                rs.getString("symbol"), rs.getString("event_type"), rs.getString("payload"), rs.getInt("attempts")))
                .stream().findFirst();
    }

    @Override
    public void markPublished(long id) {
        jdbc.update("UPDATE event_outbox SET published_at=CURRENT_TIMESTAMP(6) WHERE id=?", id);
    }

    @Override
    public void deferEvent(long id, int delaySeconds) {
        jdbc.update("UPDATE event_outbox SET attempts=attempts+1,available_at=TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6)) WHERE id=?",
                delaySeconds, id);
    }

    private static OrderEntity order(ResultSet rs, int row) throws SQLException {
        return new OrderEntity(rs.getString("order_id"), rs.getString("user_id"), rs.getString("symbol"),
                rs.getString("side"), rs.getBigDecimal("price"), rs.getBigDecimal("quantity"),
                rs.getBigDecimal("remaining"), rs.getString("status"), rs.getLong("priority"));
    }

    private static CommandEntity command(ResultSet rs, int row) throws SQLException {
        return new CommandEntity(rs.getString("command_id"), rs.getString("symbol"), rs.getLong("market_sequence"),
                rs.getString("kind"), rs.getString("order_id"), rs.getString("user_id"), rs.getString("status"),
                rs.getString("error_code"), rs.getString("message"));
    }
}
