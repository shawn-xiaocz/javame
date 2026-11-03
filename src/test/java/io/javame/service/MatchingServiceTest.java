package io.javame.service;

import io.javame.client.MatchingContracts.PlaceOrderRequest;
import io.javame.client.Side;
import io.javame.infrastructure.config.JavameProperties;
import io.javame.infrastructure.repository.MatchingRepository;
import io.javame.service.impl.MatchingServiceImpl;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Pure unit tests for service validation; no Spring context or middleware is started. */
class MatchingServiceTest {
    private MatchingRepository repository;
    private MatchingService service;
    private ValidatorFactory validators;

    @BeforeEach
    void setUp() {
        repository = mock(MatchingRepository.class);
        validators = Validation.buildDefaultValidatorFactory();
        JavameProperties properties = new JavameProperties("unit-test-service-key", "127.0.0.1", 0, "", "",
                false, false, 50, "amqp://localhost", 1000,
                Map.of("btc2usdt", new JavameProperties.Market(8, 8)));
        service = new MatchingServiceImpl(repository, properties, validators.getValidator());
    }

    @AfterEach
    void tearDown() {
        validators.close();
    }

    /** Reject malformed, non-positive, oversized or over-precision prices before storage access. */
    @ParameterizedTest
    @ValueSource(strings = {"", "-1", "NaN", "Infinity", "1e2", "0", "0.000000001", "100000000000000000000"})
    void place_shouldRejectInvalidPriceWithoutPersisting(String price) {
        assertThatThrownBy(() -> service.place(new PlaceOrderRequest("user", "order", "btc2usdt", Side.BUY, price, "1")))
                .isInstanceOf(MatchingException.class).extracting("code").isEqualTo("INVALID_ARGUMENT");
        verifyNoInteractions(repository);
    }

    /** Quantity validation shares the exact-decimal boundary rather than silently rounding. */
    @ParameterizedTest
    @ValueSource(strings = {"-1", "0", "0.000000001", "1e-8"})
    void place_shouldRejectInvalidVolumeWithoutPersisting(String volume) {
        assertThatThrownBy(() -> service.place(new PlaceOrderRequest("user", "order", "btc2usdt", Side.BUY, "1", volume)))
                .isInstanceOf(MatchingException.class);
        verifyNoInteractions(repository);
    }

    /** Symbols cannot create unconfigured markets or broker queues. */
    @Test
    void place_shouldRejectUnknownMarket() {
        assertThatThrownBy(() -> service.place(new PlaceOrderRequest("user", "order", "unknown", Side.BUY, "1", "1")))
                .isInstanceOf(MatchingException.class);
        verifyNoInteractions(repository);
    }

    /** Missing identity and direction are rejected consistently for non-HTTP callers too. */
    @Test
    void place_shouldValidateRequiredFieldsForRpcCallers() {
        assertThatThrownBy(() -> service.place(new PlaceOrderRequest("", "order", "btc2usdt", null, "1", "1")))
                .isInstanceOf(MatchingException.class);
        verifyNoInteractions(repository);
    }

    /** Invalid double-sided pagination must fail before an expensive depth query. */
    @Test
    void getDepth_shouldRejectOversizedPages() {
        assertThatThrownBy(() -> service.getDepth("btc2usdt", "DOUBLE_SIDE", 0, 101))
                .isInstanceOf(MatchingException.class);
        verifyNoInteractions(repository);
    }
}
