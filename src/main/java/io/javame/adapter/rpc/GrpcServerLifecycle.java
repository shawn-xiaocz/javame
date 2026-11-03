package io.javame.adapter.rpc;

import io.javame.infrastructure.config.JavameProperties;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/** Owns listener resources and drains active calls during application shutdown. */
@Component
public class GrpcServerLifecycle implements SmartLifecycle {
    private final JavameProperties properties;
    private final OrderGrpcAdapter order;
    private final PoolGrpcAdapter pool;
    private final ApiKeyInterceptor authentication;
    private volatile Server server;
    private ExecutorService executor;

    public GrpcServerLifecycle(JavameProperties properties, OrderGrpcAdapter order, PoolGrpcAdapter pool,
                                ApiKeyInterceptor authentication) {
        this.properties = properties;
        this.order = order;
        this.pool = pool;
        this.authentication = authentication;
    }

    @Override
    public void start() {
        boolean certificate = properties.grpcCertificate() != null && !properties.grpcCertificate().isBlank();
        boolean key = properties.grpcPrivateKey() != null && !properties.grpcPrivateKey().isBlank();
        if (certificate != key) {
            throw new IllegalArgumentException("gRPC TLS requires both certificate and private key");
        }
        executor = Executors.newFixedThreadPool(16);
        NettyServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress(properties.grpcHost(), properties.grpcPort()))
                .executor(executor).maxInboundMessageSize(16 * 1024)
                .addService(ServerInterceptors.intercept(order, authentication))
                .addService(ServerInterceptors.intercept(pool, authentication));
        if (certificate) {
            builder.useTransportSecurity(new File(properties.grpcCertificate()), new File(properties.grpcPrivateKey()));
        }
        try {
            server = builder.build().start();
        } catch (IOException | RuntimeException ex) {
            executor.shutdownNow();
            throw new IllegalStateException("Cannot start gRPC listener", ex);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    public boolean isRunning() {
        return server != null && !server.isShutdown();
    }

    public int port() {
        return server == null ? -1 : server.getPort();
    }
}
