package io.javame.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javame.client.MatchingContracts.MatchingEvent;
import io.javame.client.OrderStatus;
import io.javame.infrastructure.config.JavameProperties;
import io.javame.infrastructure.dao.CommandEntity;
import io.javame.infrastructure.dao.OrderEntity;
import io.javame.infrastructure.repository.MatchingRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** One database transaction consumes a command, changes orders, and appends all events. */
@Component
public class MatchingWorker {
    private static final Logger LOG = LoggerFactory.getLogger(MatchingWorker.class);
    private final MatchingRepository repository;
    private final JavameProperties properties;
    private final ObjectMapper mapper;
    private final TransactionTemplate transaction;

    public MatchingWorker(MatchingRepository repository, JavameProperties properties, ObjectMapper mapper,
                          PlatformTransactionManager manager) {
        this.repository = repository;
        this.properties = properties;
        this.mapper = mapper;
        this.transaction = new TransactionTemplate(manager);
        this.transaction.setTimeout(30);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    @Scheduled(fixedDelayString = "${javame.worker-delay-ms:20}")
    public void tick() {
        if (!properties.workerEnabled()) {
            return;
        }
        try {
            for (int index = 0; index < properties.batchSize() && processNext(); index++) {
                // Bound each scheduler turn; each command commits separately.
            }
        } catch (RuntimeException ex) {
            LOG.error("Matching command transaction failed; pending command will be retried", ex);
        }
    }

    /** Returns false when all eligible markets are idle or locked by another worker. */
    public boolean processNext() {
        return Boolean.TRUE.equals(transaction.execute(tx -> {
            CommandEntity command = repository.lockNextCommand().orElse(null);
            if (command == null) {
                return false;
            }
            OrderEntity order = repository.getOrder(command.orderId()).orElseThrow();
            if ("PLACE".equals(command.kind())) {
                match(command, order);
            } else {
                cancel(command, order);
            }
            return true;
        }));
    }

    private void match(CommandEntity command, OrderEntity taker) {
        if (!OrderStatus.PENDING.name().equals(taker.status())) {
            throw new IllegalStateException("Unprocessed placement has a non-pending order");
        }
        BigDecimal remaining = taker.remaining();
        while (remaining.signum() > 0) {
            OrderEntity maker = repository.getBestMaker(taker).orElse(null);
            if (maker == null) {
                break;
            }
            BigDecimal volume = remaining.min(maker.remaining());
            remaining = remaining.subtract(volume);
            BigDecimal makerRemaining = maker.remaining().subtract(volume);
            repository.updateOrder(maker.orderId(), makerRemaining, state(makerRemaining, maker.quantity()));
            UUID tradeId = UUID.randomUUID();
            Instant time = Instant.now();
            repository.insertTrade(tradeId, taker.symbol(), maker.orderId(), taker.orderId(), maker.price(), volume, time);
            emit(new MatchingEvent(1, tradeId.toString(), "TRADE", taker.symbol(), command.sequence(), taker.orderId(),
                    maker.orderId(), taker.orderId(), tradeId.toString(), plain(maker.price()), plain(volume),
                    plain(remaining), state(remaining, taker.quantity()), time));
        }
        String status = state(remaining, taker.quantity());
        repository.updateOrder(taker.orderId(), remaining, status);
        emit(new MatchingEvent(1, UUID.randomUUID().toString(), "ORDER_UPDATED", taker.symbol(), command.sequence(),
                taker.orderId(), null, null, null, plain(taker.price()), plain(taker.quantity()), plain(remaining), status, Instant.now()));
        repository.completeCommand(command.commandId(), "DONE", "", "Order processed");
    }

    private void cancel(CommandEntity command, OrderEntity order) {
        if (OrderStatus.FILLED.name().equals(order.status())) {
            repository.completeCommand(command.commandId(), "REJECTED", "ORDER_ALREADY_FILLED", "Order filled before cancellation");
            emit(new MatchingEvent(1, UUID.randomUUID().toString(), "CANCEL_REJECTED", order.symbol(), command.sequence(),
                    order.orderId(), null, null, null, plain(order.price()), "0", plain(order.remaining()), order.status(), Instant.now()));
            return;
        }
        repository.updateOrder(order.orderId(), order.remaining(), OrderStatus.CANCELLED.name());
        emit(new MatchingEvent(1, UUID.randomUUID().toString(), "ORDER_CANCELLED", order.symbol(), command.sequence(),
                order.orderId(), null, null, null, plain(order.price()), plain(order.remaining()), plain(order.remaining()),
                OrderStatus.CANCELLED.name(), Instant.now()));
        repository.completeCommand(command.commandId(), "DONE", "", "Order cancelled");
    }

    private void emit(MatchingEvent event) {
        try {
            repository.insertEvent(UUID.fromString(event.eventId()), event.symbol(), event.type(), mapper.writeValueAsString(event));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Cannot serialize matching event", ex);
        }
    }

    private static String state(BigDecimal remaining, BigDecimal original) {
        if (remaining.signum() == 0) {
            return OrderStatus.FILLED.name();
        }
        return remaining.compareTo(original) == 0 ? OrderStatus.OPEN.name() : OrderStatus.PARTIALLY_FILLED.name();
    }

    private static String plain(BigDecimal value) {
        return value.stripTrailingZeros().toPlainString();
    }
}
