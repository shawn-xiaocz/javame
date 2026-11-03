package io.javame.adapter.rpc;

import io.javame.infrastructure.config.JavameProperties;
import io.grpc.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.stereotype.Component;

/** Shared service-key authentication for every gRPC method. */
@Component
public class ApiKeyInterceptor implements ServerInterceptor {
    private static final Metadata.Key<String> HEADER = Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);
    private final byte[] expected;

    public ApiKeyInterceptor(JavameProperties properties) {
        expected = properties.apiKey().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                               ServerCallHandler<ReqT, RespT> next) {
        String supplied = headers.get(HEADER);
        if (supplied == null || !MessageDigest.isEqual(expected, supplied.getBytes(StandardCharsets.UTF_8))) {
            call.close(Status.UNAUTHENTICATED.withDescription("A valid service API key is required"), new Metadata());
            return new ServerCall.Listener<>() { };
        }
        return next.startCall(call, headers);
    }
}
