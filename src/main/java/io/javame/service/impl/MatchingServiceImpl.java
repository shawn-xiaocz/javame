package io.javame.service.impl;

import io.javame.client.MatchingContracts.*;
import io.javame.client.OrderStatus;
import io.javame.client.Side;
import io.javame.infrastructure.config.JavameProperties;
import io.javame.infrastructure.dao.CommandEntity;
import io.javame.infrastructure.dao.OrderEntity;
import io.javame.infrastructure.repository.MatchingRepository;
import io.javame.service.MatchingException;
import io.javame.service.MatchingService;
import jakarta.validation.Validator;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/** Accepts immutable idempotent commands and serves consistent order-book snapshots. */
@Service
public class MatchingServiceImpl implements MatchingService {
    private final MatchingRepository repository;
    private final JavameProperties properties;
    private final Validator validator;

    public MatchingServiceImpl(MatchingRepository repository, JavameProperties properties, Validator validator) {
        this.repository = repository;
        this.properties = properties;
        this.validator = validator;
    }

    @Override
    @Transactional(timeout = 10, isolation = Isolation.READ_COMMITTED)
    public SubmitOrderResult place(PlaceOrderRequest request) {
        validate(request);
        JavameProperties.Market market = market(request.symbol());
        BigDecimal price = decimal(request.price(), market.priceScale(), "price");
        BigDecimal volume = decimal(request.volume(), market.volumeScale(), "volume");
        repository.lockMarket(request.symbol());
        String commandId = "place:" + request.oid();
        OrderEntity existing = repository.getOrder(request.oid()).orElse(null);
        if (existing != null) {
            verifyReplay(existing, request, price, volume);
            return submitted(commandId);
        }
        long sequence = repository.nextSequence(request.symbol());
        OrderEntity order = new OrderEntity(request.oid(), request.uuid(), request.symbol(), request.transaction().name(),
                price, volume, volume, OrderStatus.PENDING.name(), sequence);
        // The primary key also protects global order IDs concurrently submitted in different markets.
        if (!repository.insertOrder(order)) {
            verifyReplay(repository.getOrder(request.oid()).orElseThrow(), request, price, volume);
            return submitted(commandId);
        }
        repository.insertCommand(new CommandEntity(commandId, request.symbol(), sequence, "PLACE", request.oid(),
                request.uuid(), "PENDING", "", ""));
        return new SubmitOrderResult(commandId, "ACCEPTED");
    }

    @Override
    @Transactional(timeout = 10, isolation = Isolation.READ_COMMITTED)
    public SubmitOrderResult cancel(CancelOrderRequest request) {
        validate(request);
        market(request.symbol());
        repository.lockMarket(request.symbol());
        OrderEntity order = ownedOrder(request.uuid(), request.oid(), request.symbol());
        String commandId = "cancel:" + request.oid();
        if (repository.getCommand(commandId).isPresent()) {
            return submitted(commandId);
        }
        if (OrderStatus.FILLED.name().equals(order.status())) {
            throw new MatchingException("ORDER_ALREADY_FILLED", "A filled order cannot be cancelled");
        }
        long sequence = repository.nextSequence(request.symbol());
        repository.insertCommand(new CommandEntity(commandId, request.symbol(), sequence, "CANCEL", request.oid(),
                request.uuid(), "PENDING", "", ""));
        return new SubmitOrderResult(commandId, "ACCEPTED");
    }

    @Override
    @Transactional(readOnly = true)
    public GetOrderResult getOrder(String uuid, String oid, String symbol) {
        validate(new CancelOrderRequest(uuid, oid, symbol));
        market(symbol);
        OrderEntity order = ownedOrder(uuid, oid, symbol);
        return new GetOrderResult(order.orderId(), order.symbol(), Side.valueOf(order.side()), plain(order.price()),
                plain(order.quantity()), plain(order.remaining()), order.status());
    }

    @Override
    @Transactional(readOnly = true)
    public GetCommandResult getCommand(String uuid, String commandId) {
        if (uuid == null || uuid.isBlank() || uuid.length() > 128 || commandId == null || commandId.length() > 160) {
            throw new MatchingException("INVALID_ARGUMENT", "Valid user and command IDs are required");
        }
        CommandEntity command = repository.getCommand(commandId)
                .filter(value -> value.userId().equals(uuid))
                .orElseThrow(() -> new MatchingException("NOT_FOUND", "Command not found"));
        return new GetCommandResult(command.commandId(), command.status(), command.errorCode(), command.message());
    }

    @Override
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public GetDepthResult getDepth(String symbol, String direction, int offset, int count) {
        market(symbol);
        if (!List.of("BUY", "SELL", "DOUBLE_SIDE").contains(Objects.toString(direction, ""))
                || offset < 0 || count < 1 || count > 100) {
            throw new MatchingException("INVALID_ARGUMENT", "Direction and pagination are invalid; count must be 1..100");
        }
        boolean buy = !"SELL".equals(direction);
        boolean sell = !"BUY".equals(direction);
        List<DepthDTO> buys = buy ? depths(symbol, "BUY", offset, count) : List.of();
        List<DepthDTO> sells = sell ? depths(symbol, "SELL", offset, count) : List.of();
        return new GetDepthResult(buy ? repository.countDepth(symbol, "BUY") : 0,
                sell ? repository.countDepth(symbol, "SELL") : 0, buys, sells);
    }

    private List<DepthDTO> depths(String symbol, String side, int offset, int count) {
        return repository.listDepth(symbol, side, offset, count).stream()
                .map(level -> new DepthDTO(plain(level.price()), plain(level.volume()))).toList();
    }

    private OrderEntity ownedOrder(String uuid, String oid, String symbol) {
        return repository.getOrder(oid).filter(order -> order.userId().equals(uuid) && order.symbol().equals(symbol))
                .orElseThrow(() -> new MatchingException("NOT_FOUND", "Order not found"));
    }

    private SubmitOrderResult submitted(String commandId) {
        CommandEntity command = repository.getCommand(commandId).orElseThrow();
        return new SubmitOrderResult(commandId, command.status());
    }

    private void verifyReplay(OrderEntity order, PlaceOrderRequest request, BigDecimal price, BigDecimal volume) {
        if (!order.userId().equals(request.uuid()) || !order.symbol().equals(request.symbol())
                || !order.side().equals(request.transaction().name()) || order.price().compareTo(price) != 0
                || order.quantity().compareTo(volume) != 0) {
            throw new MatchingException("IDEMPOTENCY_CONFLICT", "Order ID has already been used with different parameters");
        }
    }

    private JavameProperties.Market market(String symbol) {
        JavameProperties.Market market = symbol == null ? null : properties.markets().get(symbol);
        if (market == null) {
            throw new MatchingException("INVALID_ARGUMENT", "Unknown market");
        }
        return market;
    }

    private <T> void validate(T request) {
        if (request == null || !validator.validate(request).isEmpty()) {
            throw new MatchingException("INVALID_ARGUMENT", "Required order parameters are missing or invalid");
        }
    }

    private BigDecimal decimal(String text, int scale, String field) {
        // Bound length and syntax before BigDecimal allocation; never round a client's order.
        if (text == null || !text.matches("(?:0|[1-9][0-9]{0,19})(?:\\.[0-9]{1,18})?")) {
            throw new MatchingException("INVALID_ARGUMENT", field + " must be a positive plain decimal with at most 20 integer digits");
        }
        try {
            BigDecimal value = new BigDecimal(text).setScale(scale, RoundingMode.UNNECESSARY);
            if (value.signum() <= 0) {
                throw new ArithmeticException("not positive");
            }
            return value;
        } catch (ArithmeticException ex) {
            throw new MatchingException("INVALID_ARGUMENT", field + " is zero or exceeds market precision");
        }
    }

    private static String plain(BigDecimal value) {
        return value.stripTrailingZeros().toPlainString();
    }
}
