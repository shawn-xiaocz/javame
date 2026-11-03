package io.javame.service;

import io.javame.client.MatchingContracts.*;

/** Shared business entry for HTTP and gRPC callers. */
public interface MatchingService {
    SubmitOrderResult place(PlaceOrderRequest request);
    SubmitOrderResult cancel(CancelOrderRequest request);
    GetOrderResult getOrder(String uuid, String oid, String symbol);
    GetCommandResult getCommand(String uuid, String commandId);
    GetDepthResult getDepth(String symbol, String direction, int offset, int count);
}
