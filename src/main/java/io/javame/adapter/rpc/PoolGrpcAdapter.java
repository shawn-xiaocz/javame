package io.javame.adapter.rpc;

import io.javame.api.*;
import io.javame.client.MatchingContracts.*;
import io.javame.service.MatchingException;
import io.javame.service.MatchingService;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

/** Best bid descends, best ask ascends; returned quantities use their original units. */
@Component
public class PoolGrpcAdapter extends PoolGrpc.PoolImplBase {
    private final MatchingService service;

    public PoolGrpcAdapter(MatchingService service) {
        this.service = service;
    }

    @Override
    public void getDepth(DepthRequest request, StreamObserver<DepthResponse> observer) {
        GrpcCalls.respond(observer, () -> {
            if (request.getTransaction() == DepthRequest.DepthType.UNRECOGNIZED
                    || request.getOffset() < 0 || request.getOffset() > Integer.MAX_VALUE
                    || request.getCount() < 0 || request.getCount() > 100) {
                throw new MatchingException("INVALID_ARGUMENT", "Invalid depth direction or pagination");
            }
            int count = request.getCount() == 0 ? 20 : (int) request.getCount();
            GetDepthResult result = service.getDepth(request.getSymbol(), request.getTransaction().name(),
                    (int) request.getOffset(), count);
            DepthResponse.Builder response = DepthResponse.newBuilder().setCode(0).setMessage("OK")
                    .setBuyTotal(result.buyTotal()).setSellTotal(result.sellTotal())
                    .setTotal(result.buyTotal() + result.sellTotal());
            result.buys().forEach(level -> response.addBuys(depth(level)));
            result.sells().forEach(level -> response.addSells(depth(level)));
            if (request.getTransaction() == DepthRequest.DepthType.BUY) {
                response.addAllData(response.getBuysList());
            } else if (request.getTransaction() == DepthRequest.DepthType.SELL) {
                response.addAllData(response.getSellsList());
            }
            return response.build();
        });
    }

    private static Depth depth(DepthDTO level) {
        return Depth.newBuilder().setP(Double.parseDouble(level.price())).setV(Double.parseDouble(level.volume()))
                .setPriceDecimal(level.price()).setVolumeDecimal(level.volume()).build();
    }
}
