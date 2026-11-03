package io.javame.infrastructure.config;

import io.javame.infrastructure.repository.MatchingRepository;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.stereotype.Component;

/** Refuses to start replicas whose precision settings disagree with persisted markets. */
@Component
@DependsOnDatabaseInitialization
public class MarketInitializer implements InitializingBean {
    private final MatchingRepository repository;
    private final JavameProperties properties;

    public MarketInitializer(MatchingRepository repository, JavameProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        properties.markets().forEach((symbol, market) ->
                repository.registerMarket(symbol, market.priceScale(), market.volumeScale()));
    }
}
