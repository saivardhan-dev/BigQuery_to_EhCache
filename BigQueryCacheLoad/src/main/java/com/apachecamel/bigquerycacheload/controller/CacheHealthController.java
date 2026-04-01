package com.apachecamel.bigquerycacheload.controller;

import com.apachecamel.bigquerycacheload.model.MyRecord;
import com.apachecamel.bigquerycacheload.service.BigQueryCacheLoader;
import com.apachecamel.bigquerycacheload.service.CacheService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

@RestController
public class CacheHealthController {

    private static final Logger log = LoggerFactory.getLogger(CacheHealthController.class);

    private final CacheService        cacheService;
    private final BigQueryCacheLoader cacheLoader;
    private final CircuitBreaker      circuitBreaker;

    @Value("${cache.ttl-minutes:5}")
    private int ttlMinutes;

    public CacheHealthController(
            CacheService cacheService,
            BigQueryCacheLoader cacheLoader,
            CircuitBreaker circuitBreaker) {
        this.cacheService   = cacheService;
        this.cacheLoader    = cacheLoader;
        this.circuitBreaker = circuitBreaker;
    }

    // =========================================================================
    // HEALTH & STATUS
    // =========================================================================

    /**
     * GET /health/cache
     * Full cache health: circuit breaker state, last load, next reload, size, status.
     * Returns 200 HEALTHY or 503 STALE/EMPTY.
     */
    @GetMapping("/health/cache")
    public ResponseEntity<CacheHealthResponse> cacheHealth() {
        Instant lastLoad   = cacheLoader.getLastSuccessfulLoad();
        Instant nextReload = cacheLoader.getNextReloadAt();
        String  cbState    = circuitBreaker.getState().name();
        long    cacheSize  = cacheService.getCacheSize();

        double dataAgeMinutes = (lastLoad == null) ? -1.0
                : ChronoUnit.SECONDS.between(lastLoad, Instant.now()) / 60.0;

        String status = computeStatus(lastLoad, dataAgeMinutes);

        CacheHealthResponse response = new CacheHealthResponse(
                cbState,
                lastLoad   != null ? lastLoad.toString()   : "never",
                nextReload != null ? nextReload.toString() : "not scheduled",
                cacheSize,
                dataAgeMinutes < 0 ? -1.0 : Math.round(dataAgeMinutes * 100.0) / 100.0,
                status
        );

        HttpStatus httpStatus = "HEALTHY".equals(status) ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(httpStatus).body(response);
    }

    /**
     * GET /cache/status
     * Full dashboard: health + hit rate + load timing + next reload countdown.
     */
    @GetMapping("/cache/status")
    public ResponseEntity<CacheStatusResponse> cacheStatus() {
        Instant lastLoad   = cacheLoader.getLastSuccessfulLoad();
        Instant nextReload = cacheLoader.getNextReloadAt();

        double dataAgeMinutes = (lastLoad == null) ? -1.0
                : ChronoUnit.SECONDS.between(lastLoad, Instant.now()) / 60.0;

        long secondsUntilReload = (nextReload == null) ? -1
                : Math.max(0, ChronoUnit.SECONDS.between(Instant.now(), nextReload));

        String status = computeStatus(lastLoad, dataAgeMinutes);

        CacheStatusResponse response = new CacheStatusResponse(
                status,
                circuitBreaker.getState().name(),
                lastLoad   != null ? lastLoad.toString()   : "never",
                nextReload != null ? nextReload.toString() : "not scheduled",
                String.format("%02d:%02d", secondsUntilReload / 60, secondsUntilReload % 60),
                cacheService.getCacheSize(),
                dataAgeMinutes < 0 ? -1.0 : Math.round(dataAgeMinutes * 100.0) / 100.0,
                cacheService.getHitCount(),
                cacheService.getMissCount(),
                cacheService.getHitRatePercent(),
                cacheLoader.getLastLoadDurationMs(),
                cacheLoader.getLastKnownRowCount(),
                cacheLoader.getLastLoadedRowCount(),
                cacheLoader.isLastLoadWasPartial(),
                cacheLoader.getLastLoadStatus(),
                cacheLoader.getConsecutiveFailures(),
                cacheService.isCacheReady()
        );

        HttpStatus httpStatus = "HEALTHY".equals(status) ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(httpStatus).body(response);
    }

    /**
     * GET /cache/count
     * Returns just the number of entries in the cache.
     */
    @GetMapping("/cache/count")
    public ResponseEntity<CountResponse> count() {
        return ResponseEntity.ok(new CountResponse(cacheService.getCacheSize()));
    }

    /**
     * GET /cache/stats
     * Hit/miss counters, hit rate, cache size, last load duration.
     */
    @GetMapping("/cache/stats")
    public ResponseEntity<CacheStatsResponse> stats() {
        return ResponseEntity.ok(new CacheStatsResponse(
                cacheService.getCacheSize(),
                cacheService.getHitCount(),
                cacheService.getMissCount(),
                cacheService.getHitRatePercent(),
                cacheService.isCacheReady(),
                cacheLoader.getLastLoadDurationMs()
        ));
    }

    // =========================================================================
    // RECORD LOOKUP
    // =========================================================================

    /**
     * GET /cache/record/{id}
     * Look up a single record by its ID.
     * Returns 200 + record, or 404 if not found.
     */
    @GetMapping("/cache/record/{id}")
    public ResponseEntity<?> getById(@PathVariable String id) {
        Optional<MyRecord> record = cacheService.getById(id);
        if (record.isPresent()) {
            return ResponseEntity.ok(record.get());
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("No record found for id: " + id));
    }

    /**
     * GET /cache/{id}
     * Shorthand alias for /cache/record/{id}.
     */
    @GetMapping("/cache/{id}")
    public ResponseEntity<?> getByIdShort(@PathVariable String id) {
        return getById(id);
    }

    // =========================================================================
    // BROWSING & SEARCH
    // =========================================================================

    /**
     * GET /cache/all?page=0&size=50
     * Returns a paginated list of all records currently in the cache.
     * Default: page=0, size=50. Max size=500.
     */
    @GetMapping("/cache/all")
    public ResponseEntity<PageResponse> getAll(
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "50") int size) {

        size = Math.min(size, 500);
        List<MyRecord> records = cacheService.getAll(page, size);

        return ResponseEntity.ok(new PageResponse(
                records,
                page,
                size,
                records.size(),
                cacheService.getCacheSize()
        ));
    }

    /**
     * GET /cache/search?category=electronics&limit=100
     * GET /cache/search?name=Product&limit=100
     * Filter records by category or name (partial, case-insensitive).
     * Returns 400 if neither parameter is provided.
     */
    @GetMapping("/cache/search")
    public ResponseEntity<?> search(
            @RequestParam(required = false) String category,
            @RequestParam(required = false) String name,
            @RequestParam(defaultValue = "100") int limit) {

        limit = Math.min(limit, 1000);

        if (category != null && !category.isBlank()) {
            List<MyRecord> results = cacheService.searchByCategory(category, limit);
            log.debug("Search by category='{}' returned {} results.", category, results.size());
            return ResponseEntity.ok(new SearchResponse("category", category, results, results.size()));
        }

        if (name != null && !name.isBlank()) {
            List<MyRecord> results = cacheService.searchByName(name, limit);
            log.debug("Search by name='{}' returned {} results.", name, results.size());
            return ResponseEntity.ok(new SearchResponse("name", name, results, results.size()));
        }

        return ResponseEntity.badRequest()
                .body(new ErrorResponse("Provide at least one search parameter: category or name"));
    }

    // =========================================================================
    // CACHE CONTROL
    // =========================================================================

    /**
     * POST /cache/reload
     * Manually triggers an immediate cache reload from BigQuery.
     * Runs synchronously — response returns after load completes.
     */
    @PostMapping("/cache/reload")
    public ResponseEntity<ReloadResponse> reload() {
        log.info("Manual cache reload triggered via POST /cache/reload");
        long start = System.currentTimeMillis();
        cacheLoader.loadAll();
        long durationMs = System.currentTimeMillis() - start;

        String loadStatus = cacheLoader.getLastLoadStatus();
        boolean partial   = cacheLoader.isLastLoadWasPartial();

        HttpStatus status = partial ? HttpStatus.PARTIAL_CONTENT : HttpStatus.OK;

        return ResponseEntity.status(status).body(new ReloadResponse(
                partial ? "Cache reload completed with partial data" : "Cache reloaded successfully",
                durationMs,
                cacheLoader.getLastSuccessfulLoad() != null
                        ? cacheLoader.getLastSuccessfulLoad().toString() : "failed",
                cacheService.getCacheSize(),
                cacheLoader.getLastLoadedRowCount(),
                cacheLoader.getLastKnownRowCount(),
                partial,
                loadStatus
        ));
    }

    /**
     * DELETE /cache/evict/{id}
     * Removes a single record from the cache by ID.
     * Returns 200 if removed, 404 if not found.
     */
    @DeleteMapping("/cache/evict/{id}")
    public ResponseEntity<?> evictById(@PathVariable String id) {
        boolean removed = cacheService.evict(id);
        if (removed) {
            return ResponseEntity.ok(new MessageResponse("Record '" + id + "' evicted from cache."));
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("Record '" + id + "' not found in cache."));
    }

    /**
     * DELETE /cache/evict/all
     * Clears the entire cache.
     * Returns 200 with confirmation.
     */
    @DeleteMapping("/cache/evict/all")
    public ResponseEntity<MessageResponse> evictAll() {
        long sizeBefore = cacheService.getCacheSize();
        cacheService.evictAll();
        log.info("Cache cleared via DELETE /cache/evict/all. {} entries removed.", sizeBefore);
        return ResponseEntity.ok(new MessageResponse(
                "Cache cleared. " + sizeBefore + " entries removed."));
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    private String computeStatus(Instant lastLoad, double dataAgeMinutes) {
        if (lastLoad == null)            return "EMPTY";
        if (dataAgeMinutes > ttlMinutes) return "STALE";
        return "HEALTHY";
    }

    // =========================================================================
    // RESPONSE RECORDS
    // =========================================================================

    public record CacheHealthResponse(
            String circuitBreakerState,
            String lastSuccessfulLoad,
            String nextScheduledReload,
            long   cacheSize,
            double dataAgeMinutes,
            String status
    ) {}

    public record CacheStatusResponse(
            String  status,
            String  circuitBreakerState,
            String  lastSuccessfulLoad,
            String  nextScheduledReload,
            String  timeUntilNextReload,
            long    cacheSize,
            double  dataAgeMinutes,
            long    hitCount,
            long    missCount,
            double  hitRatePercent,
            long    lastLoadDurationMs,
            long    lastKnownRowCount,
            long    lastLoadedRowCount,
            boolean lastLoadWasPartial,
            String  lastLoadStatus,
            int     consecutiveFailures,
            boolean cacheReady
    ) {}

    public record CacheStatsResponse(
            long    cacheSize,
            long    hitCount,
            long    missCount,
            double  hitRatePercent,
            boolean cacheReady,
            long    lastLoadDurationMs
    ) {}

    public record PageResponse(
            List<MyRecord> records,
            int  page,
            int  pageSize,
            int  returnedCount,
            long totalCacheSize
    ) {}

    public record SearchResponse(
            String         searchField,
            String         searchValue,
            List<MyRecord> results,
            int            count
    ) {}

    public record CountResponse(long count) {}

    public record ReloadResponse(
            String  message,
            long    durationMs,
            String  lastSuccessfulLoad,
            long    cacheSize,
            long    rowsLoaded,
            long    lastKnownRowCount,
            boolean wasPartialLoad,
            String  loadStatus
    ) {}

    public record MessageResponse(String message) {}

    public record ErrorResponse(String error) {}
}