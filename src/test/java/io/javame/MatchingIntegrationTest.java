package io.javame;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import com.rabbitmq.client.Channel;
import io.javame.adapter.rpc.GrpcServerLifecycle;
import io.javame.api.*;
import io.javame.client.MatchingContracts.*;
import io.javame.client.Side;
import io.javame.infrastructure.config.JavameProperties;
import io.javame.infrastructure.mq.EventPublisher;
import io.javame.infrastructure.repository.MatchingRepository;
import io.javame.service.*;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Real MySQL/RabbitMQ integration tests cover API contracts, matching, concurrency and crash boundaries. */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"javame.worker-enabled=false", "javame.publisher-enabled=false", "javame.grpc-port=0"})
class MatchingIntegrationTest {
    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>(System.getProperty("javame.mysql.image", "mysql:8.4.8"))
            .withCommand("--default-time-zone=+00:00", "--transaction-isolation=READ-COMMITTED")
            .withUrlParam("connectionTimeZone", "UTC")
            .withUrlParam("forceConnectionTimeZoneToSession", "true");
    @Container
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", MYSQL::getJdbcUrl);
        properties.add("spring.datasource.username", MYSQL::getUsername);
        properties.add("spring.datasource.password", MYSQL::getPassword);
        properties.add("javame.api-key", () -> "integration-test-service-key");
        properties.add("javame.rabbit-uri", RABBIT::getAmqpUrl);
    }

    @Autowired MatchingService service;
    @Autowired MatchingWorker worker;
    @Autowired OutboxDispatcher dispatcher;
    @Autowired MatchingRepository repository;
    @Autowired EventPublisher publisher;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired JavameProperties properties;
    @Autowired PlatformTransactionManager transactions;
    @Autowired TestRestTemplate http;
    @Autowired GrpcServerLifecycle grpc;

    @BeforeEach
    void cleanIsolatedState() throws Exception {
        for (String table : List.of("matching_order", "matching_command", "trade", "event_outbox")) {
            jdbc.execute("TRUNCATE TABLE " + table);
        }
        jdbc.update("UPDATE market SET command_sequence=0,last_processed_at='1970-01-01 00:00:00.000000'");
        try (Connection connection = broker(); Channel channel = connection.createChannel()) {
            for (String symbol : properties.markets().keySet()) {
                channel.queueDeclare("notice:" + symbol, true, false, false, null);
                channel.queuePurge("notice:" + symbol);
            }
        }
    }

    /** Lower-priced makers win first, and equal-priced makers retain durable submission order. */
    @Test
    void match_shouldRespectPriceThenTimePriority() throws Exception {
        service.place(new PlaceOrderRequest("seller", "s1", "btc2usdt", Side.SELL, "101", "2"));
        service.place(new PlaceOrderRequest("seller", "s2", "btc2usdt", Side.SELL, "100", "1"));
        service.place(new PlaceOrderRequest("seller", "s3", "btc2usdt", Side.SELL, "101", "2"));
        drain();
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "101", "4"));
        drain();

        List<JsonNode> trades = tradeEvents();
        assertThat(trades).extracting(value -> value.get("makerOrderId").asText()).containsExactly("s2", "s1", "s3");
        assertThat(trades).extracting(value -> value.get("price").asText()).containsExactly("100", "101", "101");
        assertThat(service.getOrder("seller", "s3", "btc2usdt").remainingVolume()).isEqualTo("1");
        assertThat(service.getOrder("buyer", "b1", "btc2usdt").status()).isEqualTo("FILLED");
        assertThat(service.getDepth("btc2usdt", "SELL", 0, 20).sells()).containsExactly(new DepthDTO("101", "1"));
    }

    /** An incoming sell matches the highest bid first and never crosses its limit. */
    @Test
    void match_shouldSelectHighestBidForIncomingSell() throws Exception {
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "100", "1"));
        service.place(new PlaceOrderRequest("buyer", "b2", "btc2usdt", Side.BUY, "102", "1"));
        drain();
        service.place(new PlaceOrderRequest("seller", "s1", "btc2usdt", Side.SELL, "101", "2"));
        drain();
        assertThat(tradeEvents()).extracting(value -> value.get("makerOrderId").asText()).containsExactly("b2");
        assertThat(service.getOrder("seller", "s1", "btc2usdt").remainingVolume()).isEqualTo("1");
        assertThat(service.getOrder("buyer", "b1", "btc2usdt").status()).isEqualTo("OPEN");
    }

    /** Exact 0.1 + 0.2 consumes 0.3 without residual depth or floating-point status errors. */
    @Test
    void match_shouldUseExactDecimalArithmetic() {
        service.place(new PlaceOrderRequest("seller", "s1", "btc2usdt", Side.SELL, "0.1", "0.3"));
        drain();
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "0.1", "0.1"));
        service.place(new PlaceOrderRequest("buyer", "b2", "btc2usdt", Side.BUY, "0.1", "0.2"));
        drain();
        assertThat(service.getOrder("seller", "s1", "btc2usdt").status()).isEqualTo("FILLED");
        assertThat(service.getOrder("seller", "s1", "btc2usdt").remainingVolume()).isEqualTo("0");
        assertThat(service.getDepth("btc2usdt", "DOUBLE_SIDE", 0, 20).sells()).isEmpty();
        assertThat(jdbc.queryForObject("SELECT SUM(quantity) FROM trade", BigDecimal.class)).isEqualByComparingTo("0.3");
    }

    /** Both sides are independently paginated, aggregated and returned in best-price order. */
    @Test
    void depth_shouldReturnBothSidesInOriginalUnits() {
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "90", "0.1"));
        service.place(new PlaceOrderRequest("buyer", "b2", "btc2usdt", Side.BUY, "90", "0.2"));
        service.place(new PlaceOrderRequest("buyer", "b3", "btc2usdt", Side.BUY, "89", "1"));
        service.place(new PlaceOrderRequest("seller", "s1", "btc2usdt", Side.SELL, "100", "2"));
        service.place(new PlaceOrderRequest("seller", "s2", "btc2usdt", Side.SELL, "101", "3"));
        drain();
        GetDepthResult result = service.getDepth("btc2usdt", "DOUBLE_SIDE", 0, 20);
        assertThat(result.buys()).containsExactly(new DepthDTO("90", "0.3"), new DepthDTO("89", "1"));
        assertThat(result.sells()).containsExactly(new DepthDTO("100", "2"), new DepthDTO("101", "3"));
        GetDepthResult page = service.getDepth("btc2usdt", "DOUBLE_SIDE", 1, 1);
        assertThat(page.buyTotal()).isEqualTo(2);
        assertThat(page.sellTotal()).isEqualTo(2);
        assertThat(page.buys()).containsExactly(new DepthDTO("89", "1"));
        assertThat(page.sells()).containsExactly(new DepthDTO("101", "3"));
    }

    /** A queued cancellation follows its placement and needs only owner, order ID and market. */
    @Test
    void cancel_shouldHandlePendingOrdersIdempotently() {
        service.place(new PlaceOrderRequest("owner", "pending", "btc2usdt", Side.BUY, "100", "2"));
        SubmitOrderResult first = service.cancel(new CancelOrderRequest("owner", "pending", "btc2usdt"));
        SubmitOrderResult replay = service.cancel(new CancelOrderRequest("owner", "pending", "btc2usdt"));
        assertThat(replay.commandId()).isEqualTo(first.commandId());
        drain();
        assertThat(service.getOrder("owner", "pending", "btc2usdt").status()).isEqualTo("CANCELLED");
        assertThat(service.getDepth("btc2usdt", "BUY", 0, 20).buys()).isEmpty();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_command", Long.class)).isEqualTo(2);
        assertThat(service.getCommand("owner", first.commandId()).status()).isEqualTo("DONE");
    }

    /** Partial fills are preserved and cancellation removes only the actual remaining quantity. */
    @Test
    void cancel_shouldRemoveOnlyUnfilledQuantity() {
        service.place(new PlaceOrderRequest("seller", "s1", "btc2usdt", Side.SELL, "100", "3"));
        drain();
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "100", "1"));
        drain();
        service.cancel(new CancelOrderRequest("seller", "s1", "btc2usdt"));
        drain();
        GetOrderResult order = service.getOrder("seller", "s1", "btc2usdt");
        assertThat(order.status()).isEqualTo("CANCELLED");
        assertThat(order.remainingVolume()).isEqualTo("2");
        assertThat(service.getDepth("btc2usdt", "SELL", 0, 20).sells()).isEmpty();
        assertThat(jdbc.queryForObject("SELECT SUM(quantity) FROM trade", BigDecimal.class)).isEqualByComparingTo("1");
    }

    /** A fill sequenced before an accepted cancellation produces an explicit rejected command. */
    @Test
    void cancel_shouldReportWhenEarlierCommandFillsTheOrder() {
        service.place(new PlaceOrderRequest("seller", "s1", "btc2usdt", Side.SELL, "100", "1"));
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "100", "1"));
        SubmitOrderResult cancel = service.cancel(new CancelOrderRequest("seller", "s1", "btc2usdt"));
        drain();
        GetCommandResult result = service.getCommand("seller", cancel.commandId());
        assertThat(result.status()).isEqualTo("REJECTED");
        assertThat(result.errorCode()).isEqualTo("ORDER_ALREADY_FILLED");
        assertThat(service.getOrder("seller", "s1", "btc2usdt").status()).isEqualTo("FILLED");
    }

    /** Reusing a completed order ID cannot create another fill or replace immutable parameters. */
    @Test
    void place_shouldDeduplicateAfterProcessingAndRejectConflicts() {
        PlaceOrderRequest original = new PlaceOrderRequest("owner", "same-id", "btc2usdt", Side.BUY, "100", "1");
        service.place(original);
        drain();
        assertThat(service.place(original).status()).isEqualTo("DONE");
        assertThatThrownBy(() -> service.place(new PlaceOrderRequest("owner", "same-id", "btc2usdt", Side.BUY, "100", "2")))
                .isInstanceOf(MatchingException.class).extracting("code").isEqualTo("IDEMPOTENCY_CONFLICT");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_order", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_command", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox", Long.class)).isEqualTo(1);
    }

    /** Another user's queries and cancellation must not reveal or mutate an existing order. */
    @Test
    void cancel_shouldEnforceOrderOwnership() {
        service.place(new PlaceOrderRequest("owner", "private", "btc2usdt", Side.BUY, "100", "1"));
        assertThatThrownBy(() -> service.cancel(new CancelOrderRequest("other", "private", "btc2usdt")))
                .isInstanceOf(MatchingException.class).extracting("code").isEqualTo("NOT_FOUND");
        assertThatThrownBy(() -> service.getCommand("other", "place:private")).isInstanceOf(MatchingException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_command", Long.class)).isEqualTo(1);
    }

    /** Failure after a trade write rolls back makers, taker, trade, command and outbox together. */
    @Test
    void match_shouldRollbackAndRecoverAfterMidCommandFailure() {
        service.place(new PlaceOrderRequest("seller", "s1", "btc2usdt", Side.SELL, "100", "1"));
        drain();
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "100", "1"));
        MatchingRepository faulty = mock(MatchingRepository.class, delegatesTo(repository));
        doThrow(new IllegalStateException("Simulated failure after trade insert"))
                .when(faulty).insertEvent(any(), anyString(), anyString(), anyString());
        MatchingWorker failingWorker = new MatchingWorker(faulty, properties, mapper, transactions);
        assertThatThrownBy(failingWorker::processNext).isInstanceOf(IllegalStateException.class);

        assertThat(service.getOrder("seller", "s1", "btc2usdt").remainingVolume()).isEqualTo("1");
        assertThat(service.getOrder("buyer", "b1", "btc2usdt").status()).isEqualTo("PENDING");
        assertThat(service.getCommand("buyer", "place:b1").status()).isEqualTo("PENDING");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM trade", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox", Long.class)).isEqualTo(1);

        // A fresh worker instance uses only persisted state, just as after an application restart.
        MatchingWorker recovered = new MatchingWorker(repository, properties, mapper, transactions);
        assertThat(recovered.processNext()).isTrue();
        assertThat(service.getOrder("buyer", "b1", "btc2usdt").status()).isEqualTo("FILLED");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM trade", Long.class)).isEqualTo(1);
    }

    /** Simultaneous identical requests reserve exactly one immutable order and command. */
    @Test
    void place_shouldDeduplicateConcurrentRequests() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<SubmitOrderResult>> calls = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                calls.add(() -> service.place(new PlaceOrderRequest("owner", "concurrent", "btc2usdt", Side.BUY, "100", "1")));
            }
            for (Future<SubmitOrderResult> result : executor.invokeAll(calls)) {
                assertThat(result.get(10, TimeUnit.SECONDS).commandId()).isEqualTo("place:concurrent");
            }
        } finally {
            executor.shutdownNow();
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_order", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_command", Long.class)).isEqualTo(1);
    }

    /** Global ID uniqueness also holds when two requests lock different market rows. */
    @Test
    void place_shouldRejectConcurrentOrderIdReuseAcrossMarkets() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<String> first = executor.submit(() -> {
                start.await();
                try {
                    service.place(new PlaceOrderRequest("owner", "global-id", "btc2usdt", Side.BUY, "100", "1"));
                    return "ACCEPTED";
                } catch (MatchingException ex) {
                    return ex.code();
                }
            });
            Future<String> second = executor.submit(() -> {
                start.await();
                try {
                    service.place(new PlaceOrderRequest("owner", "global-id", "eth2usdt", Side.BUY, "100", "1"));
                    return "ACCEPTED";
                } catch (MatchingException ex) {
                    return ex.code();
                }
            });
            start.countDown();
            assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder("ACCEPTED", "IDEMPOTENCY_CONFLICT");
        } finally {
            executor.shutdownNow();
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_order", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_command", Long.class)).isEqualTo(1);
    }

    /** MySQL must not collapse case-distinct order IDs or match a different owner by case. */
    @Test
    void place_shouldPreserveCaseSensitiveIdsAndOwnership() {
        service.place(new PlaceOrderRequest("Owner", "Order", "btc2usdt", Side.BUY, "100", "1"));
        service.place(new PlaceOrderRequest("owner", "order", "btc2usdt", Side.BUY, "99", "1"));
        drain();
        assertThat(service.getOrder("Owner", "Order", "btc2usdt").price()).isEqualTo("100");
        assertThat(service.getOrder("owner", "order", "btc2usdt").price()).isEqualTo("99");
        assertThatThrownBy(() -> service.getOrder("owner", "Order", "btc2usdt"))
                .isInstanceOf(MatchingException.class).extracting("code").isEqualTo("NOT_FOUND");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_order", Long.class)).isEqualTo(2);
    }

    /** NO PAD collation preserves identifiers whose only difference is trailing whitespace. */
    @Test
    void place_shouldPreserveTrailingSpacesInOrderIds() {
        service.place(new PlaceOrderRequest("owner", "exact-id", "btc2usdt", Side.BUY, "100", "1"));
        service.place(new PlaceOrderRequest("owner", "exact-id ", "btc2usdt", Side.BUY, "99", "1"));
        drain();
        assertThat(service.getOrder("owner", "exact-id", "btc2usdt").price()).isEqualTo("100");
        assertThat(service.getOrder("owner", "exact-id ", "btc2usdt").price()).isEqualTo("99");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM matching_command", Long.class)).isEqualTo(2);
    }

    /** Re-registering a market is safe, but a replica with different precision must fail startup. */
    @Test
    void registerMarket_shouldRejectPrecisionChangesWithoutOverwritingConfiguration() {
        repository.registerMarket("btc2usdt", 8, 8);
        assertThatThrownBy(() -> repository.registerMarket("btc2usdt", 7, 8))
                .isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT price_scale FROM market WHERE symbol='btc2usdt'", Integer.class)).isEqualTo(8);
    }

    /** Independent workers coordinate through MySQL and cannot overfill or duplicate trades. */
    @Test
    void match_shouldSerializeMultipleWorkersPerMarket() throws Exception {
        for (int index = 0; index < 10; index++) {
            service.place(new PlaceOrderRequest("seller", "s" + index, "btc2usdt", Side.SELL, "100", "0.1"));
        }
        service.place(new PlaceOrderRequest("buyer", "b1", "btc2usdt", Side.BUY, "100", "1"));
        MatchingWorker another = new MatchingWorker(repository, properties, mapper, transactions);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(this::drain);
            Future<?> second = executor.submit(() -> { while (another.processNext()) { } });
            first.get(20, TimeUnit.SECONDS);
            second.get(20, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
        drain();
        assertThat(service.getOrder("buyer", "b1", "btc2usdt").status()).isEqualTo("FILLED");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM trade", Long.class)).isEqualTo(10);
        assertThat(jdbc.queryForObject("SELECT SUM(quantity) FROM trade", BigDecimal.class)).isEqualByComparingTo("1");
        assertThat(service.getDepth("btc2usdt", "DOUBLE_SIDE", 0, 20).sells()).isEmpty();
    }

    /** Confirmed events are persistent JSON messages; manual requeue preserves their event IDs. */
    @Test
    void outbox_shouldPublishPersistentConfirmedEvents() throws Exception {
        service.place(new PlaceOrderRequest("owner", "event-order", "btc2usdt", Side.BUY, "100", "1"));
        drain();
        assertThat(dispatcher.dispatchNext()).isTrue();
        try (Connection connection = broker(); Channel channel = connection.createChannel()) {
            GetResponse message = channel.basicGet("notice:btc2usdt", false);
            assertThat(message).isNotNull();
            assertThat(message.getProps().getDeliveryMode()).isEqualTo(2);
            assertThat(message.getProps().getContentType()).isEqualTo("application/json");
            JsonNode body = mapper.readTree(message.getBody());
            assertThat(message.getProps().getMessageId()).isEqualTo(body.get("eventId").asText());
            assertThat(body.get("orderId").asText()).isEqualTo("event-order");
            channel.basicNack(message.getEnvelope().getDeliveryTag(), false, true);
            GetResponse redelivered = channel.basicGet("notice:btc2usdt", false);
            assertThat(redelivered.getProps().getMessageId()).isEqualTo(message.getProps().getMessageId());
            channel.basicAck(redelivered.getEnvelope().getDeliveryTag(), false);
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox WHERE published_at IS NOT NULL", Long.class)).isEqualTo(1);
    }

    /** A broker outage keeps events durable and prevents a later event overtaking its market head. */
    @Test
    void outbox_shouldBackoffWithoutLosingEventsOrReorderingMarket() throws Exception {
        service.place(new PlaceOrderRequest("owner", "event1", "btc2usdt", Side.BUY, "100", "1"));
        service.place(new PlaceOrderRequest("owner", "event2", "btc2usdt", Side.BUY, "99", "1"));
        drain();
        EventPublisher unavailable = mock(EventPublisher.class);
        doThrow(new IOException("Simulated broker outage")).when(unavailable).publish(any());
        OutboxDispatcher failed = new OutboxDispatcher(repository, unavailable, properties, transactions);
        assertThat(failed.dispatchNext()).isTrue();
        assertThat(failed.dispatchNext()).isFalse();
        assertThat(jdbc.queryForObject("SELECT SUM(attempts) FROM event_outbox", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox WHERE published_at IS NOT NULL", Long.class)).isZero();
        jdbc.update("UPDATE event_outbox SET available_at='1970-01-01 00:00:00.000000'");
        assertThat(dispatcher.dispatchNext()).isTrue();
        assertThat(dispatcher.dispatchNext()).isTrue();
        try (Connection connection = broker(); Channel channel = connection.createChannel()) {
            assertThat(mapper.readTree(channel.basicGet("notice:btc2usdt", true).getBody()).get("orderId").asText()).isEqualTo("event1");
            assertThat(mapper.readTree(channel.basicGet("notice:btc2usdt", true).getBody()).get("orderId").asText()).isEqualTo("event2");
        }
    }

    /** A delayed head blocks only its market; independent markets can continue publishing. */
    @Test
    void outbox_shouldContinueOtherMarketsWhenOneMarketIsDelayed() throws Exception {
        service.place(new PlaceOrderRequest("owner", "blocked", "btc2usdt", Side.BUY, "100", "1"));
        service.place(new PlaceOrderRequest("owner", "healthy", "eth2usdt", Side.BUY, "100", "1"));
        drain();
        EventPublisher selectivelyUnavailable = mock(EventPublisher.class);
        doAnswer(invocation -> {
            io.javame.infrastructure.dao.OutboxEntity event = invocation.getArgument(0);
            if (event.symbol().equals("btc2usdt")) {
                throw new IOException("Simulated single-market failure");
            }
            return null;
        }).when(selectivelyUnavailable).publish(any());
        OutboxDispatcher partial = new OutboxDispatcher(repository, selectivelyUnavailable, properties, transactions);
        assertThat(partial.dispatchNext()).isTrue();
        assertThat(partial.dispatchNext()).isTrue();
        assertThat(partial.dispatchNext()).isFalse();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox WHERE symbol='btc2usdt' AND published_at IS NOT NULL", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox WHERE symbol='eth2usdt' AND published_at IS NOT NULL", Long.class)).isEqualTo(1);
    }

    /** Concurrent publishers skip a locked market head without overtaking it or blocking another market. */
    @Test
    void outbox_shouldSkipLockedHeadAndContinueAnotherMarket() throws Exception {
        service.place(new PlaceOrderRequest("owner", "btc-head", "btc2usdt", Side.BUY, "100", "1"));
        service.place(new PlaceOrderRequest("owner", "btc-tail", "btc2usdt", Side.BUY, "99", "1"));
        service.place(new PlaceOrderRequest("owner", "eth-head", "eth2usdt", Side.BUY, "100", "1"));
        drain();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TransactionTemplate transaction = new TransactionTemplate(transactions);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> held = executor.submit(() -> transaction.executeWithoutResult(status -> {
            assertThat(repository.lockNextEvent().orElseThrow().symbol()).isEqualTo("btc2usdt");
            locked.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to release the test lock");
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(ex);
            }
        }));
        try {
            assertThat(locked.await(5, TimeUnit.SECONDS)).isTrue();
            EventPublisher confirmed = mock(EventPublisher.class);
            OutboxDispatcher another = new OutboxDispatcher(repository, confirmed, properties, transactions);
            assertThat(another.dispatchNext()).isTrue();
            assertThat(another.dispatchNext()).isFalse();
            var event = org.mockito.ArgumentCaptor.forClass(io.javame.infrastructure.dao.OutboxEntity.class);
            verify(confirmed).publish(event.capture());
            assertThat(event.getValue().symbol()).isEqualTo("eth2usdt");
            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox WHERE symbol='btc2usdt' AND published_at IS NOT NULL", Long.class)).isZero();
        } finally {
            release.countDown();
            try {
                held.get(10, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
            }
        }
    }

    /** A crash after broker confirm replays the same event ID, allowing downstream deduplication. */
    @Test
    void outbox_shouldRetainEventIdWhenConfirmationPrecedesDatabaseFailure() throws Exception {
        service.place(new PlaceOrderRequest("owner", "event-order", "btc2usdt", Side.BUY, "100", "1"));
        drain();
        MatchingRepository faulty = mock(MatchingRepository.class, delegatesTo(repository));
        doThrow(new IllegalStateException("Simulated crash before commit")).when(faulty).markPublished(anyLong());
        OutboxDispatcher failing = new OutboxDispatcher(faulty, publisher, properties, transactions);
        assertThatThrownBy(failing::dispatchNext).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM event_outbox WHERE published_at IS NOT NULL", Long.class)).isZero();
        assertThat(dispatcher.dispatchNext()).isTrue();
        try (Connection connection = broker(); Channel channel = connection.createChannel()) {
            GetResponse first = channel.basicGet("notice:btc2usdt", true);
            GetResponse second = channel.basicGet("notice:btc2usdt", true);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            assertThat(second.getProps().getMessageId()).isEqualTo(first.getProps().getMessageId());
        }
    }

    /** HTTP authentication, validation, accepted status and exact-decimal JSON are real wire checks. */
    @Test
    void http_shouldAuthenticateValidateAndExposeDurableState() {
        assertThat(http.getForEntity("/v1/depth/btc2usdt", String.class).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(http.getForEntity("/actuator/health", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Api-Key", properties.apiKey());
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {"uuid":"owner","oid":"http-order","symbol":"btc2usdt","transaction":"BUY","price":"0.12345678","volume":"0.1"}
                """;
        ResponseEntity<SubmitOrderResult> response = http.exchange("/v1/orders", HttpMethod.POST,
                new HttpEntity<>(body, headers), SubmitOrderResult.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().commandId()).isEqualTo("place:http-order");
        assertThat(service.getOrder("owner", "http-order", "btc2usdt").status()).isEqualTo("PENDING");
        drain();
        ResponseEntity<GetOrderResult> order = http.exchange("/v1/orders/http-order?uuid=owner&symbol=btc2usdt",
                HttpMethod.GET, new HttpEntity<>(headers), GetOrderResult.class);
        assertThat(order.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(order.getBody().price()).isEqualTo("0.12345678");
        assertThat(order.getBody().status()).isEqualTo("OPEN");
        assertThat(http.exchange("/v1/orders", HttpMethod.POST, new HttpEntity<>("{}", headers), ErrorResult.class)
                .getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    /** Existing RPC method names and doubles still work; new strings preserve large exact prices. */
    @Test
    void grpc_shouldPreserveLegacyCallsAndAddExactDecimals() throws Exception {
        ManagedChannel raw = ManagedChannelBuilder.forAddress("127.0.0.1", grpc.port()).usePlaintext().build();
        try {
            assertThatThrownBy(() -> OrderGrpc.newBlockingStub(raw).doOrder(OrderRequest.getDefaultInstance()))
                    .isInstanceOf(StatusRuntimeException.class).satisfies(ex ->
                            assertThat(((StatusRuntimeException) ex).getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
            Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER), properties.apiKey());
            io.grpc.Channel authenticated = ClientInterceptors.intercept(raw, MetadataUtils.newAttachHeadersInterceptor(headers));
            OrderGrpc.OrderBlockingStub order = OrderGrpc.newBlockingStub(authenticated).withDeadlineAfter(10, TimeUnit.SECONDS);
            order.doOrder(OrderRequest.newBuilder().setUuid("owner").setOid("legacy").setSymbol("btc2usdt")
                    .setTransaction(TransactionType.BUY).setPrice(100).setVolume(0.1).build());
            order.doOrder(OrderRequest.newBuilder().setUuid("owner").setOid("exact").setSymbol("btc2usdt")
                    .setTransaction(TransactionType.BUY).setPriceDecimal("9007199254740993.00000001").setVolumeDecimal("0.1").build());
            drain();
            DepthResponse depth = PoolGrpc.newBlockingStub(authenticated).getDepth(DepthRequest.newBuilder()
                    .setSymbol("btc2usdt").setTransaction(DepthRequest.DepthType.BUY).build());
            assertThat(depth.getData(0).getPriceDecimal()).isEqualTo("9007199254740993.00000001");
            assertThat(depth.getData(1).getP()).isEqualTo(100);
            assertThat(depth.getData(1).getV()).isEqualTo(0.1);
            // Legacy cancellation price/volume are deliberately irrelevant to order lookup.
            order.deleteOrder(OrderRequest.newBuilder().setUuid("owner").setOid("legacy").setSymbol("btc2usdt")
                    .setPrice(999).setVolume(999).build());
            drain();
            assertThat(order.getOrder(OrderQueryRequest.newBuilder().setUuid("owner").setOid("legacy")
                    .setSymbol("btc2usdt").build()).getStatus()).isEqualTo("CANCELLED");
        } finally {
            raw.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private void drain() {
        for (int index = 0; index < 1000; index++) {
            if (!worker.processNext()) {
                return;
            }
        }
        throw new AssertionError("Command drain did not converge");
    }

    private List<JsonNode> tradeEvents() throws Exception {
        List<JsonNode> events = new ArrayList<>();
        for (String payload : jdbc.queryForList("SELECT payload FROM event_outbox WHERE event_type='TRADE' ORDER BY id", String.class)) {
            events.add(mapper.readTree(payload));
        }
        return events;
    }

    private Connection broker() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(RABBIT.getAmqpUrl());
        return factory.newConnection();
    }
}
