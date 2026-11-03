package io.javame.adapter.rpc;

import io.javame.api.*;
import io.javame.client.MatchingContracts.*;
import io.javame.client.Side;
import io.javame.service.MatchingException;
import io.javame.service.MatchingService;
import io.grpc.stub.StreamObserver;
import java.math.BigDecimal;
import org.springframework.stereotype.Component;

/** Preserves the original unqualified Order service and its existing wire field numbers. */
@Component
public class OrderGrpcAdapter extends OrderGrpc.OrderImplBase {
    private final MatchingService service;

    public OrderGrpcAdapter(MatchingService service) {
        this.service = service;
    }

    @Override
    public void doOrder(OrderRequest request, StreamObserver<OrderResponse> observer) {
        GrpcCalls.respond(observer, () -> {
            Side side = switch (request.getTransaction()) {
                case BUY -> Side.BUY;
                case SELL -> Side.SELL;
                default -> throw new MatchingException("INVALID_ARGUMENT", "Invalid order direction");
            };
            SubmitOrderResult result = service.place(new PlaceOrderRequest(request.getUuid(), request.getOid(),
                    request.getSymbol(), side, decimal(request.getPriceDecimal(), request.getPrice()),
                    decimal(request.getVolumeDecimal(), request.getVolume())));
            return response(result);
        });
    }

    @Override
    public void deleteOrder(OrderRequest request, StreamObserver<OrderResponse> observer) {
        GrpcCalls.respond(observer, () -> response(service.cancel(
                new CancelOrderRequest(request.getUuid(), request.getOid(), request.getSymbol()))));
    }

    @Override
    public void getOrder(OrderQueryRequest request, StreamObserver<OrderStateResponse> observer) {
        GrpcCalls.respond(observer, () -> {
            GetOrderResult result = service.getOrder(request.getUuid(), request.getOid(), request.getSymbol());
            return OrderStateResponse.newBuilder().setOid(result.oid()).setSymbol(result.symbol())
                    .setTransaction(TransactionType.valueOf(result.transaction().name())).setPrice(result.price())
                    .setVolume(result.volume()).setRemainingVolume(result.remainingVolume()).setStatus(result.status()).build();
        });
    }

    @Override
    public void getCommand(CommandQueryRequest request, StreamObserver<CommandStateResponse> observer) {
        GrpcCalls.respond(observer, () -> {
            GetCommandResult result = service.getCommand(request.getUuid(), request.getCommandId());
            return CommandStateResponse.newBuilder().setCommandId(result.commandId()).setStatus(result.status())
                    .setErrorCode(result.errorCode()).setMessage(result.message()).build();
        });
    }

    private static OrderResponse response(SubmitOrderResult result) {
        return OrderResponse.newBuilder().setCode(0).setMessage("Command committed; query status for the outcome")
                .setCommandId(result.commandId()).setStatus(result.status()).build();
    }

    private static String decimal(String exact, double legacy) {
        if (!exact.isEmpty()) {
            return exact;
        }
        if (!Double.isFinite(legacy)) {
            throw new MatchingException("INVALID_ARGUMENT", "Price and volume must be finite");
        }
        // Legacy double precision is already lost at the caller; all new arithmetic is decimal.
        return BigDecimal.valueOf(legacy).toPlainString();
    }
}
