package com.apachecamel.bigquerycacheload.config;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.time.Duration;

@Configuration
public class CacheConfig {

    @Value("${cache.disk-path:./cache-data}")
    private String diskPath;

    @Value("${resilience4j.circuitbreaker.instances.bigquery.failure-rate-threshold:50}")
    private float failureRateThreshold;

    @Value("${resilience4j.circuitbreaker.instances.bigquery.wait-duration-in-open-state:30s}")
    private String waitDurationInOpenState;

    @Value("${resilience4j.circuitbreaker.instances.bigquery.sliding-window-size:10}")
    private int slidingWindowSize;

    // -------------------------------------------------------------------------
    // CacheManager — backed by EhCache 3 via JSR-107
    // -------------------------------------------------------------------------
    @Bean
    public CacheManager cacheManager() throws Exception {
        CachingProvider provider = Caching.getCachingProvider(
                "org.ehcache.jsr107.EhcacheCachingProvider");

        URI configUri = getClass().getClassLoader()
                .getResource("ehcache.xml")
                .toURI();

        System.setProperty("cache.disk.path", diskPath);

        javax.cache.CacheManager jCacheManager =
                provider.getCacheManager(configUri, getClass().getClassLoader());

        return new JCacheCacheManager(jCacheManager);
    }

    // -------------------------------------------------------------------------
    // Raw javax.cache.CacheManager — needed for direct Cache<K,V> access
    // -------------------------------------------------------------------------
    @Bean(name = "jsr107CacheManager")
    public javax.cache.CacheManager jsr107CacheManager() throws Exception {
        CachingProvider provider = Caching.getCachingProvider(
                "org.ehcache.jsr107.EhcacheCachingProvider");

        URI configUri = getClass().getClassLoader()
                .getResource("ehcache.xml")
                .toURI();

        System.setProperty("cache.disk.path", diskPath);

        return provider.getCacheManager(configUri, getClass().getClassLoader());
    }

    // -------------------------------------------------------------------------
    // CircuitBreaker
    // -------------------------------------------------------------------------
    @Bean
    public CircuitBreaker bigQueryCircuitBreaker(CircuitBreakerRegistry registry) {
        long waitSeconds = parseDurationSeconds(waitDurationInOpenState);

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(slidingWindowSize)
                .waitDurationInOpenState(Duration.ofSeconds(waitSeconds))
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        return registry.circuitBreaker("bigquery", config);
    }

    // -------------------------------------------------------------------------
    // Retry
    // -------------------------------------------------------------------------
    @Bean
    public Retry bigQueryRetry(RetryRegistry registry) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(2), 2.0))
                .build();

        return registry.retry("bigquery", config);
    }

    // -------------------------------------------------------------------------
    // BigQuery client — uses Application Default Credentials
    // -------------------------------------------------------------------------
    @Bean
    public BigQuery bigQuery(@Value("${bigquery.project-id}") String projectId) {
        return BigQueryOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    private long parseDurationSeconds(String value) {
        if (value == null || value.isBlank()) return 30L;
        String cleaned = value.trim().toLowerCase();
        if (cleaned.endsWith("s")) {
            return Long.parseLong(cleaned.substring(0, cleaned.length() - 1).trim());
        }
        return Long.parseLong(cleaned);
    }
}