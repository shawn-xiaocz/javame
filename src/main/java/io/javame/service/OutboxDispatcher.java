package io.javame.service;

import io.javame.infrastructure.config.JavameProperties;
import io.javame.infrastructure.dao.OutboxEntity;
import io.javame.infrastructure.mq.EventPublisher;
import io.javame.infrastructure.repository.MatchingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** At-least-once outbox delivery. Broker outages never roll back completed trades. */
@Component
public class OutboxDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxDispatcher.class);
    private final MatchingRepository repository;
    private final EventPublisher publisher;
    private final JavameProperties properties;
    private final TransactionTemplate transaction;

    public OutboxDispatcher(MatchingRepository repository, EventPublisher publisher,
                            JavameProperties properties, PlatformTransactionManager manager) {
        this.repository = repository;
        this.publisher = publisher;
        this.properties = properties;
        transaction = new TransactionTemplate(manager);
        transaction.setTimeout(30);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    @Scheduled(fixedDelayString = "${javame.publisher-delay-ms:100}")
    public void tick() {
        if (!properties.publisherEnabled()) {
            return;
        }
        try {
            for (int index = 0; index < properties.batchSize() && dispatchNext(); index++) {
                // Each confirmed event commits separately, limiting replay after a crash.
            }
        } catch (RuntimeException ex) {
            LOG.error("Outbox transaction failed; uncommitted publication will be retried", ex);
        }
    }

    /** Locks only a market's oldest unpublished event, preserving per-market publication order. */
    public boolean dispatchNext() {
        return Boolean.TRUE.equals(transaction.execute(tx -> {
            OutboxEntity event = repository.lockNextEvent().orElse(null);
            if (event == null) {
                return false;
            }
            try {
                publisher.publish(event);
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                int delaySeconds = 1 << Math.min(event.attempts(), 6);
                repository.deferEvent(event.id(), delaySeconds);
                LOG.warn("Matching event publication deferred: eventId={}, symbol={}, attempts={}",
                        event.eventId(), event.symbol(), event.attempts() + 1, ex);
                return true;
            }
            // A crash after broker confirmation but before this commit can duplicate the same event ID.
            repository.markPublished(event.id());
            return true;
        }));
    }
}
