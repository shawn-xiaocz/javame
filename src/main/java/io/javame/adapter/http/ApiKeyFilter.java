package io.javame.adapter.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javame.client.MatchingContracts.ErrorResult;
import io.javame.infrastructure.config.JavameProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Requires service authentication before invoking business endpoints. */
@Component
public class ApiKeyFilter extends OncePerRequestFilter {
    private final byte[] expected;
    private final ObjectMapper mapper;

    public ApiKeyFilter(JavameProperties properties, ObjectMapper mapper) {
        expected = properties.apiKey().getBytes(StandardCharsets.UTF_8);
        this.mapper = mapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // Use the container's normalized route, not a caller-controlled raw URI prefix.
        return request.getServletPath().equals("/actuator/health");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String supplied = request.getHeader("X-Api-Key");
        if (supplied == null || !MessageDigest.isEqual(expected, supplied.getBytes(StandardCharsets.UTF_8))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType("application/json");
            mapper.writeValue(response.getOutputStream(), new ErrorResult("UNAUTHENTICATED", "A valid service API key is required"));
            return;
        }
        chain.doFilter(request, response);
    }
}
