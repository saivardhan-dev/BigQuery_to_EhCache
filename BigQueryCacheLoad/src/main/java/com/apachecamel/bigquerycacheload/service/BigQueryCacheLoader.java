package com.apachecamel.bigquerycacheload.service;

import com.apachecamel.bigquerycacheload.model.MyRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Service
public class BigQueryCacheLoader {

    private static final Logger log       = LoggerFactory.getLogger(BigQueryCacheLoader.class);
    private static final String CACHE_NAME = "myRecordCache";

    // ── Fix 4: how many consecutive failures before escalation
    private static final int ESCALATION_THRESHOLD = 3;

    // ── Fix 2: if new load brings < this fraction of last known count, reject it
    private static final double PARTIAL_LOAD_THRESHOLD = 0.10;

    private final BigQuery       bigQuery;
    private final CacheManager   cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry          retry;
    private final ObjectMapper   objectMapper = new ObjectMapper();

    @Value("${bigquery.project-id}") private String  projectId;
    @Value("${bigquery.dataset}")    private String  dataset;
    @Value("${bigquery.table}")      private String  table;
    @Value("${cache.loader.batch-size:10000}") private int     batchSize;
    @Value("${cache.loader.parallel:false}")   private boolean parallelLoad;
    @Value("${cache.loader.threads:4}")        private int     loaderThreads;
    @Value("${cache.ttl-minutes:5}")           private int     ttlMinutes;

    // ── Volatile state — written by loader thread, read by countdown + health threads
    private volatile Instant lastSuccessfulLoad  = null;
    private volatile long    lastLoadDurationMs  = -1;
    private volatile Instant nextReloadAt        = null;
    private volatile boolean loadInProgress      = false;
    private volatile long    lastKnownRowCount   = 0;       // Fix 2: track last good count
    private volatile long    lastLoadedRowCount  = 0;       // Fix 2: count from most recent attempt
    private volatile boolean lastLoadWasPartial  = false;   // Fix 2: partial load flag
    private volatile String  lastLoadStatus      = "NEVER_LOADED"; // overall load status string

    // ── Fix 4: consecutive failure counter — AtomicInteger for thread safety
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    public BigQueryCacheLoader(
            BigQuery bigQuery,
            @Qualifier("jsr107CacheManager") CacheManager cacheManager,
            CircuitBreaker circuitBreaker,
            Retry retry) {
        this.bigQuery       = bigQuery;
        this.cacheManager   = cacheManager;
        this.circuitBreaker = circuitBreaker;
        this.retry          = retry;
    }

    // -------------------------------------------------------------------------
    // Startup warm — async, non-blocking
    // -------------------------------------------------------------------------
    @PostConstruct
    @Async
    public void warmCacheOnStartup() {
        log.info("Starting async cache warm-up from BigQuery...");
        loadInProgress = true;
        executeLoad();
        loadInProgress = false;
        scheduleNextReload();
    }

    // -------------------------------------------------------------------------
    // Fix 3 — reloadLoop wrapped in try/catch so scheduler thread NEVER dies
    // -------------------------------------------------------------------------
    @Scheduled(fixedRate = 1000)
    public void reloadLoop() {
        try {
            if (nextReloadAt == null || loadInProgress) return;

            Instant now = Instant.now();

            if (now.isAfter(nextReloadAt)) {
                System.out.println();
                log.info("── Scheduled reload triggered. Reloading from BigQuery...");
                loadInProgress = true;
                loadAll();
                loadInProgress = false;
                scheduleNextReload();
            } else {
                printCountdown(now);
            }
        } catch (Exception ex) {
            // Fix 3 — catch anything unexpected so the @Scheduled thread pool
            // never loses this task. Log and continue — next tick will retry.
            log.error("Unexpected error in reloadLoop — scheduler thread protected. " +
                    "Will retry on next tick. Error: {}", ex.getMessage(), ex);
        }
    }

    // -------------------------------------------------------------------------
    // Public load entry — CB + Retry wrapper
    // -------------------------------------------------------------------------
    public void loadAll() {
        Supplier<Void> decorated = CircuitBreaker.decorateSupplier(
                circuitBreaker,
                Retry.decorateSupplier(retry, () -> {
                    executeLoad();
                    return null;
                }));
        try {
            decorated.get();
        } catch (Exception ex) {
            handleLoadFailure(ex);
        }
    }

    // -------------------------------------------------------------------------
    // Core load logic
    // -------------------------------------------------------------------------
    void executeLoad() {
        log.info("Executing BigQuery cache load (parallel={})...", parallelLoad);
        Cache<String, MyRecord> cache = cacheManager.getCache(
                CACHE_NAME, String.class, MyRecord.class);

        // Clear existing entries before reload so we write into a clean cache.
        // Without this, EhCache must evict + overwrite every entry which gets
        // progressively slower as disk tier fragments over multiple cycles.
        log.info("Clearing existing cache entries before reload...");
        long clearStart = System.currentTimeMillis();
        cache.clear();
        log.info("Cache cleared in {}ms. Loading fresh data from BigQuery...",
                System.currentTimeMillis() - clearStart);

        long startTime = System.currentTimeMillis();
        long rowsLoaded;

        if (parallelLoad) {
            rowsLoaded = loadParallel(cache);
        } else {
            rowsLoaded = loadSequential(cache);
        }

        long elapsedMs = System.currentTimeMillis() - startTime;

        // ── Fix 2: partial load detection
        if (isPartialLoad(rowsLoaded)) {
            lastLoadWasPartial  = true;
            lastLoadedRowCount  = rowsLoaded;
            lastLoadStatus      = String.format(
                    "PARTIAL — loaded %,d rows (%.1f%% of last known %,d)",
                    rowsLoaded,
                    (lastKnownRowCount > 0 ? rowsLoaded * 100.0 / lastKnownRowCount : 0),
                    lastKnownRowCount);

            log.warn("PARTIAL LOAD DETECTED: loaded {} rows but last known count was {}. " +
                            "Threshold is {}%. Old cache data is preserved.",
                    rowsLoaded, lastKnownRowCount,
                    (int)(PARTIAL_LOAD_THRESHOLD * 100));
            log.warn("Load status: {}", lastLoadStatus);

            // do NOT update lastSuccessfulLoad — old data stays intact
            consecutiveFailures.incrementAndGet();
            checkEscalation("Partial load — " + lastLoadStatus);
            return;
        }

        // ── Fix 5: empty cache guard
        if (rowsLoaded == 0) {
            lastLoadStatus = "EMPTY — zero rows returned from BigQuery";
            log.error("CRITICAL: Cache load returned 0 rows. " +
                    "BigQuery may be empty or the query may have failed silently. " +
                    "Old cache data is preserved.");
            consecutiveFailures.incrementAndGet();
            checkEscalation("Empty load — zero rows returned");
            return;
        }

        // ── Successful load
        lastKnownRowCount   = rowsLoaded;
        lastLoadedRowCount  = rowsLoaded;
        lastLoadWasPartial  = false;
        lastLoadDurationMs  = elapsedMs;
        lastSuccessfulLoad  = Instant.now();
        lastLoadStatus      = String.format("OK — %,d rows loaded in %dms", rowsLoaded, elapsedMs);
        consecutiveFailures.set(0); // Fix 4: reset on success

        log.info("Cache load complete in {}ms ({} seconds). Rows: {}. lastSuccessfulLoad={}",
                elapsedMs, String.format("%.2f", elapsedMs / 1000.0),
                rowsLoaded, lastSuccessfulLoad);

        alertIfStale();
    }

    // -------------------------------------------------------------------------
    // Fix 2: partial load check
    // -------------------------------------------------------------------------
    private boolean isPartialLoad(long rowsLoaded) {
        if (lastKnownRowCount == 0) return false; // first ever load — no baseline
        double fraction = (double) rowsLoaded / lastKnownRowCount;
        return fraction < PARTIAL_LOAD_THRESHOLD;
    }

    // -------------------------------------------------------------------------
    // Sequential load — returns number of rows loaded
    // -------------------------------------------------------------------------
    private long loadSequential(Cache<String, MyRecord> cache) {
        log.info("Sending query to BigQuery...");
        long queryStart  = System.currentTimeMillis();
        TableResult result = runQuery(buildQuery());
        long queryMs     = System.currentTimeMillis() - queryStart;
        log.info("BigQuery query returned in {}ms. Total rows: {}", queryMs, result.getTotalRows());

        log.info("Starting cache insert...");
        long insertStart = System.currentTimeMillis();
        Map<String, MyRecord> batch = new HashMap<>(batchSize);
        long totalRows = 0;

        for (FieldValueList row : result.iterateAll()) {
            MyRecord record = mapToRecord(row);
            batch.put(record.getId(), record);
            totalRows++;
            if (batch.size() >= batchSize) {
                cache.putAll(batch);
                log.debug("Flushed batch of {} records. Total so far: {}", batch.size(), totalRows);
                batch = new HashMap<>(batchSize);
            }
        }
        if (!batch.isEmpty()) {
            cache.putAll(batch);
            log.debug("Flushed final batch of {} records.", batch.size());
        }

        long insertMs = System.currentTimeMillis() - insertStart;
        log.info("Cache insert complete in {}ms. Total rows loaded: {}", insertMs, totalRows);
        log.info("Timing breakdown — BQ query: {}ms | Cache insert: {}ms | Total: {}ms",
                queryMs, insertMs, queryMs + insertMs);
        return totalRows;
    }

    // -------------------------------------------------------------------------
    // Parallel load — returns total rows loaded across all partitions
    // -------------------------------------------------------------------------
    private long loadParallel(Cache<String, MyRecord> cache) {
        ExecutorService executor = Executors.newFixedThreadPool(loaderThreads);
        List<Future<Map<String, MyRecord>>> futures = new ArrayList<>();

        for (String partitionQuery : buildPartitions()) {
            futures.add(executor.submit(() -> loadPartition(partitionQuery)));
        }
        executor.shutdown();

        long total = 0;
        for (Future<Map<String, MyRecord>> future : futures) {
            try {
                Map<String, MyRecord> part = future.get(10, TimeUnit.MINUTES);
                cache.putAll(part);
                total += part.size();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Parallel load interrupted", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new RuntimeException("Parallel partition load failed", e);
            }
        }
        log.info("Parallel load finished across 4 partitions. Total rows: {}", total);
        return total;
    }

    private Map<String, MyRecord> loadPartition(String query) {
        TableResult result = runQuery(query);
        Map<String, MyRecord> data = new HashMap<>(batchSize);
        for (FieldValueList row : result.iterateAll()) {
            MyRecord r = mapToRecord(row);
            data.put(r.getId(), r);
        }
        return data;
    }

    private String[] buildPartitions() {
        String base = String.format(
                "SELECT id, name, category, value, updated_at FROM `%s.%s`", dataset, table);
        return new String[]{
                base + " WHERE MOD(ABS(FARM_FINGERPRINT(id)), 4) = 0",
                base + " WHERE MOD(ABS(FARM_FINGERPRINT(id)), 4) = 1",
                base + " WHERE MOD(ABS(FARM_FINGERPRINT(id)), 4) = 2",
                base + " WHERE MOD(ABS(FARM_FINGERPRINT(id)), 4) = 3"
        };
    }

    // -------------------------------------------------------------------------
    // Fix 1 — runQuery with exception type detection
    // -------------------------------------------------------------------------
    private TableResult runQuery(String sql) {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
                .setUseLegacySql(false)
                .build();
        try {
            return bigQuery.query(config);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("BQ_ERROR [INTERRUPTED] Query interrupted. " +
                    "This usually means the JVM is shutting down.");
            throw new RuntimeException("BigQuery query was interrupted", e);

        } catch (BigQueryException e) {
            int code = e.getCode();
            String msg = e.getMessage();

            if (code == 401 || code == 403) {
                log.error("BQ_ERROR [AUTH] Authentication/authorisation failure (HTTP {}). " +
                        "Check ADC credentials or IAM roles. Message: {}", code, msg);
            } else if (code == 429) {
                log.error("BQ_ERROR [QUOTA] Quota exceeded (HTTP 429). " +
                        "Too many queries or bytes processed. Message: {}", msg);
            } else if (code == 503 || code == 500) {
                log.error("BQ_ERROR [SERVICE] BigQuery service error (HTTP {}). " +
                        "Google-side outage or transient fault. Message: {}", code, msg);
            } else if (code == 404) {
                log.error("BQ_ERROR [NOT_FOUND] Dataset or table not found (HTTP 404). " +
                        "Check bigquery.dataset and bigquery.table config. Message: {}", msg);
            } else {
                log.error("BQ_ERROR [UNKNOWN] Unexpected BigQuery error (HTTP {}). Message: {}",
                        code, msg);
            }
            throw e;

        } catch (Exception e) {
            Throwable cause = e.getCause();

            if (cause instanceof SocketTimeoutException || cause instanceof java.net.SocketTimeoutException) {
                log.error("BQ_ERROR [TIMEOUT] Network socket timed out connecting to BigQuery. " +
                        "Check network connectivity. Message: {}", e.getMessage());
            } else if (cause instanceof IOException) {
                log.error("BQ_ERROR [NETWORK] I/O error communicating with BigQuery. " +
                        "Possible network drop or DNS failure. Message: {}", e.getMessage());
            } else {
                log.error("BQ_ERROR [RUNTIME] Unexpected runtime error during BigQuery query. " +
                        "Message: {}", e.getMessage(), e);
            }
            throw new RuntimeException("BigQuery query failed", e);
        }
    }

    private String buildQuery() {
        return String.format(
                "SELECT id, name, category, value, updated_at FROM `%s.%s`",
                dataset, table);
    }

    // -------------------------------------------------------------------------
    // Row mapper
    // -------------------------------------------------------------------------
    MyRecord mapToRecord(FieldValueList row) {
        return new MyRecord(
                getStringOrDefault(row, "id",         ""),
                getStringOrDefault(row, "name",       ""),
                getStringOrDefault(row, "category",   ""),
                getLongOrDefault(  row, "value",      0L),
                getInstantOrDefault(row, "updated_at", Instant.EPOCH)
        );
    }

    private String getStringOrDefault(FieldValueList row, String field, String def) {
        try {
            FieldValue fv = row.get(field);
            return (fv == null || fv.isNull()) ? def : fv.getStringValue();
        } catch (Exception e) {
            log.warn("Failed to read string field '{}', using default.", field);
            return def;
        }
    }

    private long getLongOrDefault(FieldValueList row, String field, long def) {
        try {
            FieldValue fv = row.get(field);
            return (fv == null || fv.isNull()) ? def : fv.getLongValue();
        } catch (Exception e) {
            log.warn("Failed to read long field '{}', using default.", field);
            return def;
        }
    }

    private Instant getInstantOrDefault(FieldValueList row, String field, Instant def) {
        try {
            FieldValue fv = row.get(field);
            if (fv == null || fv.isNull()) return def;
            double epochMicros = Double.parseDouble(fv.getStringValue());
            return Instant.ofEpochMilli((long) (epochMicros * 1000));
        } catch (Exception e) {
            log.warn("Failed to read timestamp field '{}', using default.", field);
            return def;
        }
    }

    // -------------------------------------------------------------------------
    // Failure handling — never rethrows to caller
    // -------------------------------------------------------------------------
    void handleLoadFailure(Throwable ex) {
        String cbState = circuitBreaker.getState().name();
        lastLoadStatus = "FAILED — " + ex.getMessage();

        log.error("BigQuery cache load failed after all retries. " +
                "CircuitBreaker state={}. Error: {}", cbState, ex.getMessage(), ex);

        // Fix 4: increment and check escalation
        int failures = consecutiveFailures.incrementAndGet();
        checkEscalation("Load failure #" + failures + " — " + ex.getMessage());

        notifyOps(ex, cbState);

        if (lastSuccessfulLoad != null) {
            double ageMin = ChronoUnit.SECONDS.between(lastSuccessfulLoad, Instant.now()) / 60.0;
            log.warn("Continuing to serve stale cache data. Age: {:.2f} minutes. " +
                    "Last known row count: {}", ageMin, lastKnownRowCount);
        } else {
            log.error("No successful load has ever completed. Cache is EMPTY.");
        }
    }

    // -------------------------------------------------------------------------
    // Fix 4 — escalation after ESCALATION_THRESHOLD consecutive failures
    // -------------------------------------------------------------------------
    private void checkEscalation(String reason) {
        int failures = consecutiveFailures.get();
        if (failures >= ESCALATION_THRESHOLD) {
            log.error("ESCALATION ALERT: {} consecutive failures detected. Reason: {}. " +
                            "Immediate investigation required. CircuitBreaker={}. " +
                            "Last successful load: {}. Last known row count: {}",
                    failures, reason,
                    circuitBreaker.getState().name(),
                    lastSuccessfulLoad != null ? lastSuccessfulLoad : "NEVER",
                    lastKnownRowCount);
            notifyOps(new RuntimeException("Escalation: " + reason), circuitBreaker.getState().name());
        }
    }

    // -------------------------------------------------------------------------
    // Staleness alerting
    // -------------------------------------------------------------------------
    public void alertIfStale() {
        if (lastSuccessfulLoad == null) {
            log.error("Cache has never been successfully loaded.");
            return;
        }
        double ageMinutes = ChronoUnit.SECONDS.between(lastSuccessfulLoad, Instant.now()) / 60.0;
        if (ageMinutes > ttlMinutes * 2) {
            log.error("CACHE CRITICALLY STALE: {:.2f} min old (threshold: {}min).",
                    ageMinutes, ttlMinutes * 2);
        } else if (ageMinutes > ttlMinutes) {
            log.warn("CACHE STALE: {:.2f} min old (threshold: {}min).",
                    ageMinutes, ttlMinutes);
        }
    }

    // -------------------------------------------------------------------------
    // Structured ops notification — logs JSON alert
    // -------------------------------------------------------------------------
    public void notifyOps(Throwable ex, String cbState) {
        double ageMin = (lastSuccessfulLoad == null) ? -1.0
                : ChronoUnit.SECONDS.between(lastSuccessfulLoad, Instant.now()) / 60.0;

        Map<String, Object> alert = new LinkedHashMap<>();
        alert.put("timestamp",           Instant.now().toString());
        alert.put("cbState",             cbState);
        alert.put("consecutiveFailures", consecutiveFailures.get());
        alert.put("errorMessage",        ex != null ? ex.getMessage() : "unknown");
        alert.put("lastSuccessfulLoad",  lastSuccessfulLoad != null ? lastSuccessfulLoad.toString() : "never");
        alert.put("dataAgeMinutes",      String.format("%.2f", ageMin));
        alert.put("lastKnownRowCount",   lastKnownRowCount);
        alert.put("lastLoadedRowCount",  lastLoadedRowCount);
        alert.put("loadWasPartial",      lastLoadWasPartial);
        alert.put("loadStatus",          lastLoadStatus);

        try {
            log.error("OPS_ALERT: {}", objectMapper.writeValueAsString(alert));
        } catch (Exception jsonEx) {
            log.error("OPS_ALERT (raw): cbState={}, failures={}, error={}, lastLoad={}, ageMin={:.2f}",
                    cbState, consecutiveFailures.get(),
                    ex != null ? ex.getMessage() : "unknown",
                    lastSuccessfulLoad, ageMin);
        }
    }

    // -------------------------------------------------------------------------
    // Countdown printer
    // -------------------------------------------------------------------------
    private void printCountdown(Instant now) {
        long secondsRemaining = ChronoUnit.SECONDS.between(now, nextReloadAt);
        if (secondsRemaining < 0) secondsRemaining = 0;
        System.out.printf("\r                                                                 Next cache reload in: %02d:%02d     ",
                secondsRemaining / 60, secondsRemaining % 60);
        System.out.flush();
    }

    private void scheduleNextReload() {
        nextReloadAt = Instant.now().plus(Duration.ofMinutes(ttlMinutes));
        log.info("Next reload scheduled at: {} (in {} minutes)", nextReloadAt, ttlMinutes);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------
    public Instant getLastSuccessfulLoad()  { return lastSuccessfulLoad; }
    public long    getLastLoadDurationMs()  { return lastLoadDurationMs; }
    public Instant getNextReloadAt()        { return nextReloadAt; }
    public long    getLastKnownRowCount()   { return lastKnownRowCount; }
    public long    getLastLoadedRowCount()  { return lastLoadedRowCount; }
    public boolean isLastLoadWasPartial()   { return lastLoadWasPartial; }
    public String  getLastLoadStatus()      { return lastLoadStatus; }
    public int     getConsecutiveFailures() { return consecutiveFailures.get(); }
}