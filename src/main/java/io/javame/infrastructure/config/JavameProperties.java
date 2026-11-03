package io.javame.infrastructure.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Explicit market configuration prevents arbitrary queue and symbol creation. */
@Validated
@ConfigurationProperties("javame")
public record JavameProperties(
        @NotBlank @Size(min = 16, max = 256) String apiKey,
        @NotBlank String grpcHost,
        @Min(0) @Max(65535) int grpcPort,
        String grpcCertificate,
        String grpcPrivateKey,
        boolean workerEnabled,
        boolean publisherEnabled,
        @Min(1) @Max(1000) int batchSize,
        @NotBlank String rabbitUri,
        @Min(100) @Max(30000) int rabbitConfirmTimeoutMs,
        @NotEmpty Map<@Pattern(regexp = "[a-z0-9][a-z0-9_-]{0,63}") String, @Valid Market> markets) {

    public record Market(@Min(0) @Max(18) int priceScale, @Min(0) @Max(18) int volumeScale) { }

    /** Credentials must never appear in configuration dumps. */
    @Override
    public String toString() {
        return "JavameProperties[markets=" + markets.keySet() + "]";
    }
}
