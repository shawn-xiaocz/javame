package io.javame.adapter.rpc;

import io.javame.service.MatchingException;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

/** Standard gRPC errors replace the old non-standard panic code and unsafe response logging. */
final class GrpcCalls {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcCalls.class);

    private GrpcCalls() { }

    static <T> void respond(StreamObserver<T> observer, Supplier<T> call) {
        if (Context.current().isCancelled()) {
            observer.onError(Status.CANCELLED.asRuntimeException());
            return;
        }
        try {
            T response = call.get();
            observer.onNext(response);
            observer.onCompleted();
        } catch (MatchingException ex) {
            Status status = switch (ex.code()) {
                case "INVALID_ARGUMENT" -> Status.INVALID_ARGUMENT;
                case "NOT_FOUND" -> Status.NOT_FOUND;
                case "IDEMPOTENCY_CONFLICT" -> Status.ALREADY_EXISTS;
                default -> Status.FAILED_PRECONDITION;
            };
            observer.onError(status.withDescription(ex.getMessage()).asRuntimeException());
        } catch (DataAccessException | TransactionException ex) {
            LOG.error("gRPC matching storage transaction failed", ex);
            observer.onError(Status.UNAVAILABLE.withDescription("Storage unavailable; retry using the same order ID").asRuntimeException());
        } catch (RuntimeException ex) {
            LOG.error("gRPC matching request failed", ex);
            observer.onError(Status.INTERNAL.withDescription("Request could not be processed").asRuntimeException());
        }
    }
}
