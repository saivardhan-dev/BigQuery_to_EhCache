package com.apachecamel.bigquerycacheload.metrics;

import com.apachecamel.bigquerycacheload.service.BigQueryCacheLoader;
import com.apachecamel.bigquerycacheload.service.CacheService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
public class CacheMetrics {

    private final MeterRegistry       meterRegistry;
    private final CacheService        cacheService;
    private final BigQueryCacheLoader cacheLoader;
    private final CircuitBreaker      circuitBreaker;

    public CacheMetrics(
            MeterRegistry meterRegistry,
            CacheService cacheService,
            BigQueryCacheLoader cacheLoader,
            CircuitBreaker circuitBreaker) {
        this.meterRegistry  = meterRegistry;
        this.cacheService   = cacheService;
        this.cacheLoader    = cacheLoader;
        this.circuitBreaker = circuitBreaker;
    }

    @PostConstruct
    public void registerGauges() {

        // Gauge 1 — current number of entries in the cache
        Gauge.builder("cache.size", cacheService, CacheService::getCacheSize)
                .description("Number of entries currently held in the EhCache")
                .register(meterRegistry);

        // Gauge 2 — age of the cache data in hours (-1 if never loaded)
        Gauge.builder("cache.data.age.hours", cacheLoader, loader -> {
                    Instant lastLoad = loader.getLastSuccessfulLoad();
                    if (lastLoad == null) return -1.0;
                    return ChronoUnit.SECONDS.between(lastLoad, Instant.now()) / 3600.0;
                })
                .description("Age of the cached data in hours since last successful BigQuery load")
                .register(meterRegistry);

        // Gauge 3 — circuit breaker state: 0=CLOSED, 1=HALF_OPEN, 2=OPEN
        Gauge.builder("circuit.breaker.state", circuitBreaker, cb -> {
                    switch (cb.getState()) {
                        case CLOSED:    return 0.0;
                        case HALF_OPEN: return 1.0;
                        case OPEN:      return 2.0;
                        default:        return -1.0;
                    }
                })
                .description("Circuit breaker state: 0=CLOSED, 1=HALF_OPEN, 2=OPEN")
                .tag("name", "bigquery")
                .register(meterRegistry);
    }
}