package io.javame.infrastructure.mq;

import io.javame.infrastructure.dao.OutboxEntity;

/** A successful return requires broker confirmation and a routable persistent delivery. */
public interface EventPublisher {
    void publish(OutboxEntity event) throws Exception;
}
