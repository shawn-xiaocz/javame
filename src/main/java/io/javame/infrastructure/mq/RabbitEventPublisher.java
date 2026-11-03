package io.javame.infrastructure.mq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.javame.infrastructure.config.JavameProperties;
import io.javame.infrastructure.dao.OutboxEntity;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;

/** Reuses a connection/channel; serializes confirms so each event has an unambiguous outcome. */
@Component
public class RabbitEventPublisher implements EventPublisher {
    private final JavameProperties properties;
    private final Set<String> declaredQueues = new HashSet<>();
    private final AtomicBoolean returned = new AtomicBoolean();
    private Connection connection;
    private Channel channel;

    public RabbitEventPublisher(JavameProperties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized void publish(OutboxEntity event) throws Exception {
        try {
            connect();
            String queue = "notice:" + event.symbol();
            if (declaredQueues.add(queue)) {
                channel.queueDeclare(queue, true, false, false, null);
            }
            returned.set(false);
            AMQP.BasicProperties headers = new AMQP.BasicProperties.Builder()
                    .contentType("application/json").contentEncoding("utf-8").deliveryMode(2)
                    .messageId(event.eventId().toString()).type(event.type()).build();
            channel.basicPublish("", queue, true, headers, event.payload().getBytes(StandardCharsets.UTF_8));
            channel.waitForConfirmsOrDie(properties.rabbitConfirmTimeoutMs());
            if (returned.get()) {
                throw new IOException("Broker returned an unroutable matching event");
            }
        } catch (Exception ex) {
            close();
            throw ex;
        }
    }

    private void connect() throws Exception {
        if (connection != null && connection.isOpen() && channel != null && channel.isOpen()) {
            return;
        }
        close();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(properties.rabbitUri());
        factory.setConnectionTimeout(properties.rabbitConfirmTimeoutMs());
        factory.setHandshakeTimeout(properties.rabbitConfirmTimeoutMs());
        factory.setChannelRpcTimeout(properties.rabbitConfirmTimeoutMs());
        factory.setRequestedHeartbeat(30);
        factory.setAutomaticRecoveryEnabled(false);
        connection = factory.newConnection("javame-outbox");
        channel = connection.createChannel();
        channel.confirmSelect();
        channel.addReturnListener(message -> returned.set(true));
    }

    /** Abort avoids indefinite shutdown while a broker or network is unavailable. */
    @PreDestroy
    public synchronized void close() {
        if (connection != null) {
            connection.abort();
        }
        connection = null;
        channel = null;
        declaredQueues.clear();
    }
}
