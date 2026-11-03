package io.javame.client;

import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/** Public contracts use decimal strings to preserve precision in every client language. */
public final class MatchingContracts {
    private MatchingContracts() { }

    public record PlaceOrderRequest(
            @NotBlank @Size(max = 128) String uuid,
            @NotBlank @Size(max = 128) String oid,
            @NotBlank @Size(max = 64) String symbol,
            @NotNull Side transaction,
            @NotBlank @Size(max = 64) String price,
            @NotBlank @Size(max = 64) String volume) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public record CancelOrderRequest(
            @NotBlank @Size(max = 128) String uuid,
            @NotBlank @Size(max = 128) String oid,
            @NotBlank @Size(max = 64) String symbol) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /** ACCEPTED means committed to the durable inbox; it does not mean filled. */
    public record SubmitOrderResult(String commandId, String status) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public record GetOrderResult(String oid, String symbol, Side transaction, String price,
                                 String volume, String remainingVolume, String status) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public record GetCommandResult(String commandId, String status, String errorCode,
                                   String message) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public record DepthDTO(String price, String volume) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /** Offset/count apply independently to each side when direction is DOUBLE_SIDE. */
    public record GetDepthResult(long buyTotal, long sellTotal, List<DepthDTO> buys,
                                 List<DepthDTO> sells) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public record ErrorResult(String code, String message) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /** Stable event IDs allow consumers to deduplicate at-least-once deliveries. */
    public record MatchingEvent(int schemaVersion, String eventId, String type, String symbol,
                                long commandSequence, String orderId, String makerOrderId,
                                String takerOrderId, String tradeId, String price, String volume,
                                String remainingVolume, String status, Instant occurredAt) { }
}
