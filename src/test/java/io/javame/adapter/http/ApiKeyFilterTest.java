package io.javame.adapter.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javame.infrastructure.config.JavameProperties;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import static org.assertj.core.api.Assertions.*;

/** Unit tests for normalized HTTP authentication paths; no server or middleware is started. */
class ApiKeyFilterTest {
    private ApiKeyFilter filter;

    @BeforeEach
    void setUp() {
        JavameProperties properties = new JavameProperties("unit-test-service-key", "127.0.0.1", 0, "", "",
                false, false, 50, "amqp://localhost", 1000,
                Map.of("btc2usdt", new JavameProperties.Market(8, 8)));
        filter = new ApiKeyFilter(properties, new ObjectMapper());
    }

    /** A raw health prefix cannot bypass authentication for a normalized business route. */
    @Test
    void filter_shouldAuthenticateNormalizedBusinessRoute() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/actuator/health/../../v1/orders");
        request.setServletPath("/v1/orders");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request, response, chain);
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(chain.getRequest()).isNull();
    }

    /** Health checks are the sole unauthenticated route. */
    @Test
    void filter_shouldAllowExactHealthRoute() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setServletPath("/actuator/health");
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request, new MockHttpServletResponse(), chain);
        assertThat(chain.getRequest()).isSameAs(request);
    }

    /** A valid service credential allows the business adapter to execute. */
    @Test
    void filter_shouldAllowValidServiceKey() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setServletPath("/v1/orders");
        request.addHeader("X-Api-Key", "unit-test-service-key");
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request, new MockHttpServletResponse(), chain);
        assertThat(chain.getRequest()).isSameAs(request);
    }

    /** An incorrect credential cannot reach the controller. */
    @Test
    void filter_shouldRejectIncorrectServiceKey() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setServletPath("/v1/orders");
        request.addHeader("X-Api-Key", "invalid");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request, response, chain);
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(chain.getRequest()).isNull();
    }
}
