package com.apachecamel.bigquerycacheload.service;

import com.apachecamel.bigquerycacheload.model.MyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CacheService {

    private static final Logger log      = LoggerFactory.getLogger(CacheService.class);
    private static final String CACHE_NAME = "myRecordCache";

    private final CacheManager        cacheManager;
    private final BigQueryCacheLoader cacheLoader;

    private final AtomicLong hitCount  = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

    public CacheService(
            @Qualifier("jsr107CacheManager") CacheManager cacheManager,
            BigQueryCacheLoader cacheLoader) {
        this.cacheManager = cacheManager;
        this.cacheLoader  = cacheLoader;
    }

    // -------------------------------------------------------------------------
    // Single record lookup
    // -------------------------------------------------------------------------
    public Optional<MyRecord> getById(String id) {
        if (id == null || id.isBlank()) {
            log.warn("getById called with null or blank id.");
            return Optional.empty();
        }
        MyRecord record = getCache().get(id);
        if (record != null) {
            hitCount.incrementAndGet();
            log.debug("Cache HIT  for id='{}'. hits={}, misses={}", id, hitCount.get(), missCount.get());
            return Optional.of(record);
        }
        missCount.incrementAndGet();
        log.debug("Cache MISS for id='{}'. hits={}, misses={}", id, hitCount.get(), missCount.get());
        return Optional.empty();
    }

    // -------------------------------------------------------------------------
    // Get all records — paginated (page is 0-based)
    // -------------------------------------------------------------------------
    public List<MyRecord> getAll(int page, int pageSize) {
        List<MyRecord> result = new ArrayList<>();
        int skip = page * pageSize;
        int count = 0;

        for (Cache.Entry<String, MyRecord> entry : getCache()) {
            if (count < skip) {
                count++;
                continue;
            }
            result.add(entry.getValue());
            if (result.size() >= pageSize) break;
            count++;
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Search by category (case-insensitive)
    // -------------------------------------------------------------------------
    public List<MyRecord> searchByCategory(String category, int limit) {
        if (category == null || category.isBlank()) return Collections.emptyList();
        String lower = category.toLowerCase();
        List<MyRecord> result = new ArrayList<>();

        for (Cache.Entry<String, MyRecord> entry : getCache()) {
            if (entry.getValue().getCategory() != null &&
                    entry.getValue().getCategory().toLowerCase().contains(lower)) {
                result.add(entry.getValue());
                if (result.size() >= limit) break;
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Search by name (case-insensitive, partial match)
    // -------------------------------------------------------------------------
    public List<MyRecord> searchByName(String name, int limit) {
        if (name == null || name.isBlank()) return Collections.emptyList();
        String lower = name.toLowerCase();
        List<MyRecord> result = new ArrayList<>();

        for (Cache.Entry<String, MyRecord> entry : getCache()) {
            if (entry.getValue().getName() != null &&
                    entry.getValue().getName().toLowerCase().contains(lower)) {
                result.add(entry.getValue());
                if (result.size() >= limit) break;
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Evict a single record by id
    // -------------------------------------------------------------------------
    public boolean evict(String id) {
        boolean removed = getCache().remove(id);
        if (removed) log.info("Evicted record id='{}'", id);
        else         log.warn("Evict requested for id='{}' but not found in cache.", id);
        return removed;
    }

    // -------------------------------------------------------------------------
    // Clear entire cache
    // -------------------------------------------------------------------------
    public void evictAll() {
        getCache().clear();
        log.info("Cache cleared entirely.");
    }

    // -------------------------------------------------------------------------
    // Count entries
    // -------------------------------------------------------------------------
    public long getCacheSize() {
        long count = 0;
        for (Cache.Entry<String, MyRecord> ignored : getCache()) count++;
        return count;
    }

    // -------------------------------------------------------------------------
    // State checks
    // -------------------------------------------------------------------------
    public boolean isCacheReady() {
        return cacheLoader.getLastSuccessfulLoad() != null;
    }

    public long getHitCount()  { return hitCount.get(); }
    public long getMissCount() { return missCount.get(); }

    public double getHitRatePercent() {
        long total = hitCount.get() + missCount.get();
        if (total == 0) return 0.0;
        return Math.round((hitCount.get() * 100.0 / total) * 100.0) / 100.0;
    }

    private Cache<String, MyRecord> getCache() {
        return cacheManager.getCache(CACHE_NAME, String.class, MyRecord.class);
    }
}