# BigQuery → EhCache

A production-ready **Spring Boot 3** application that continuously loads data from **Google BigQuery** into a **3-tier EhCache** (heap + off-heap + disk), with full resilience, observability, and a REST API for cache inspection and control.

---

## Table of contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Prerequisites](#prerequisites)
- [GCP setup](#gcp-setup)
- [Configuration](#configuration)
- [Running the application](#running-the-application)
- [REST API reference](#rest-api-reference)
- [How it works](#how-it-works)
- [Resilience](#resilience)
- [Observability](#observability)
- [Performance](#performance)

---

## Overview

This application solves a common problem in high-throughput systems — querying a large dataset repeatedly from a database is slow and expensive. Instead, this app:

1. Loads **1 million+ records** from BigQuery on startup (asynchronously — app starts instantly)
2. Stores them in a **3-tier EhCache** (heap → off-heap → disk) for microsecond reads
3. Automatically **refreshes every 5 minutes** by clearing and reloading from BigQuery
4. Shows a **live countdown timer** in the console ticking down to the next reload
5. Wraps every BigQuery call with **CircuitBreaker + Retry** so the app never crashes on connection failures
6. Exposes a full **REST API** for health checks, record lookups, search, and cache control

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│               Google BigQuery                   │
│        cache_dataset.cache_table                │
│           1,000,005 rows                        │
└──────────────────┬──────────────────────────────┘
                   │  SELECT * (every 5 minutes)
                   ▼
┌─────────────────────────────────────────────────┐
│         Resilience4j Protection Layer           │
│   CircuitBreaker (50% threshold, 10-call window)│
│   Retry (3 attempts, exponential backoff 2s)    │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────┐
│           BigQueryCacheLoader                   │
│  • Async startup warm-up (@PostConstruct)       │
│  • Batch insert — 10,000 rows per putAll()      │
│  • Countdown timer (\r overwrite every second)  │
│  • Partial load detection (< 10% threshold)     │
│  • Consecutive failure escalation alerts        │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────┐
│           EhCache 3 — Tiered Storage            │
│  Heap: 100,000 entries  (nanoseconds)           │
│  Off-heap: 1 GB         (microseconds)          │
│  Disk: 10 GB persistent (milliseconds)          │
│  TTL: 5 minutes                                 │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────┐
│        REST API — CacheHealthController         │
│  Health · Status · Lookup · Search · Control   │
└─────────────────────────────────────────────────┘
```

---

## Tech stack

| Component | Technology | Version |
|---|---|---|
| Framework | Spring Boot | 3.2.4 |
| Language | Java | 17 |
| Cache | EhCache 3 (JSR-107) | 3.10.8 |
| Data source | Google BigQuery Java Client | 2.38.0 |
| Resilience | Resilience4j | 2.2.0 |
| Metrics | Micrometer + Prometheus | 1.12.4 |
| Build | Maven | 3.8+ |

---

## Project structure

```
BigQueryCacheLoad/
├── src/main/java/com/apachecamel/bigquerycacheload/
│   ├── BigQueryCacheApplication.java       # Entry point
│   ├── config/
│   │   └── CacheConfig.java               # EhCache, CircuitBreaker, Retry, BigQuery beans
│   ├── controller/
│   │   └── CacheHealthController.java     # All REST endpoints
│   ├── metrics/
│   │   └── CacheMetrics.java              # Micrometer gauge registration
│   ├── model/
│   │   └── MyRecord.java                  # Serializable POJO (id, name, category, value, updatedAt)
│   └── service/
│       ├── BigQueryCacheLoader.java        # Core loader with scheduling and resilience
│       └── CacheService.java              # Cache access layer with hit/miss tracking
├── src/main/resources/
│   ├── application.yml                    # All configuration (externalized)
│   └── ehcache.xml                        # EhCache tier configuration
└── pom.xml
```

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Google Cloud SDK (`gcloud` CLI)
- A GCP project with BigQuery API enabled
- Application Default Credentials (ADC) configured

---

## GCP setup

### 1. Install Google Cloud SDK

```bash
brew install --cask google-cloud-sdk
```

### 2. Authenticate

```bash
gcloud auth application-default login
gcloud config set project bigquery-to-ehcache
gcloud auth application-default set-quota-project bigquery-to-ehcache
```

### 3. Enable APIs

```bash
gcloud services enable bigquery.googleapis.com serviceusage.googleapis.com \
  --project=bigquery-to-ehcache
```

### 4. Create dataset and table

```bash
bq mk --dataset --location=US bigquery-to-ehcache:cache_dataset

bq mk --table \
  bigquery-to-ehcache:cache_dataset.cache_table \
  id:STRING,name:STRING,category:STRING,value:INTEGER,updated_at:TIMESTAMP
```

### 5. Load sample data (optional — 1 million rows)

Generate a CSV and load it:

```bash
bq load \
  --source_format=CSV \
  --skip_leading_rows=0 \
  --project_id=bigquery-to-ehcache \
  bigquery-to-ehcache:cache_dataset.cache_table \
  /path/to/records.csv \
  id:STRING,name:STRING,category:STRING,value:INTEGER,updated_at:TIMESTAMP
```

---

## Configuration

All values are externalized in `src/main/resources/application.yml`:

```yaml
bigquery:
  project-id: bigquery-to-ehcache   # GCP project ID
  dataset: cache_dataset             # BigQuery dataset name
  table: cache_table                 # BigQuery table name

cache:
  ttl-minutes: 5                     # Cache TTL and reload interval
  heap-entries: 100000               # Max entries in heap tier
  offheap-gb: 1                      # Off-heap size in GB
  disk-gb: 10                        # Disk tier size in GB
  disk-path: ./cache-data            # Disk persistence path
  loader:
    batch-size: 10000                # Rows per putAll() batch
    parallel: false                  # Enable 4-partition parallel load
    threads: 4                       # Threads for parallel load

resilience4j:
  circuitbreaker:
    instances:
      bigquery:
        failure-rate-threshold: 50
        sliding-window-size: 10
        wait-duration-in-open-state: 30s
  retry:
    instances:
      bigquery:
        max-attempts: 3
        wait-duration: 2s
        enable-exponential-backoff: true
```

---

## Running the application

```bash
# Clone the repo
git clone https://github.com/saivardhan-dev/BigQuery_to_EhCache.git
cd BigQuery_to_EhCache/BigQueryCacheLoad

# Build
mvn clean package -DskipTests

# Run
mvn spring-boot:run
```

The app starts immediately on port `8080`. The cache warms up asynchronously in the background — you will see the live countdown timer in the console once the first load completes:

```
INFO  Starting async cache warm-up from BigQuery...
INFO  BigQuery query returned in 7716ms. Total rows: 1000005
INFO  Cache insert complete in 62806ms. Total rows loaded: 1000005
INFO  Timing breakdown — BQ query: 7716ms | Cache insert: 62806ms | Total: 70522ms
INFO  Next reload scheduled at: 2026-04-01T15:48:57Z (in 5 minutes)
  Next cache reload in: 04:59
```

---

## REST API reference

### Health & status

| Method | Endpoint | Description | Response |
|---|---|---|---|
| `GET` | `/health/cache` | Circuit breaker state, last load, next reload, status | `200` HEALTHY / `503` STALE or EMPTY |
| `GET` | `/cache/status` | Full dashboard — countdown, hit rate, load timing, failures | `200` / `503` |
| `GET` | `/cache/count` | Number of entries currently in cache | `200` |
| `GET` | `/cache/stats` | Hit count, miss count, hit rate %, last load duration | `200` |

### Record lookup

| Method | Endpoint | Description | Response |
|---|---|---|---|
| `GET` | `/cache/record/{id}` | Look up a single record by ID | `200` record / `404` not found |
| `GET` | `/cache/{id}` | Shorthand alias for above | `200` record / `404` not found |

### Browsing & search

| Method | Endpoint | Description | Response |
|---|---|---|---|
| `GET` | `/cache/all?page=0&size=50` | Paginated list of all cached records | `200` page |
| `GET` | `/cache/search?category=electronics` | Filter by category (partial, case-insensitive) | `200` results |
| `GET` | `/cache/search?name=Product&limit=100` | Filter by name (partial, case-insensitive) | `200` results |

### Cache control

| Method | Endpoint | Description | Response |
|---|---|---|---|
| `POST` | `/cache/reload` | Trigger immediate reload from BigQuery | `200` / `206` partial |
| `DELETE` | `/cache/evict/{id}` | Remove a single record from cache | `200` / `404` |
| `DELETE` | `/cache/evict/all` | Clear the entire cache | `200` |

### Example responses

**`GET /cache/status`**
```json
{
  "status": "HEALTHY",
  "circuitBreakerState": "CLOSED",
  "lastSuccessfulLoad": "2026-04-01T14:32:46Z",
  "nextScheduledReload": "2026-04-01T14:37:46Z",
  "timeUntilNextReload": "03:42",
  "cacheSize": 1000005,
  "dataAgeMinutes": 1.23,
  "hitCount": 4821,
  "missCount": 12,
  "hitRatePercent": 99.75,
  "lastLoadDurationMs": 70522,
  "lastKnownRowCount": 1000005,
  "consecutiveFailures": 0,
  "cacheReady": true
}
```

**`GET /cache/record/rec-0000001`**
```json
{
  "id": "rec-0000001",
  "name": "Product 1",
  "category": "software",
  "value": 19680,
  "updatedAt": "2026-04-01T09:00:00Z"
}
```

---

## How it works

### Startup sequence

1. Spring Boot starts Tomcat and initializes all beans — takes ~1 second
2. `@PostConstruct` fires `warmCacheOnStartup()` on a background thread via `@Async`
3. App is immediately available on port 8080 (returns `503 EMPTY` until load completes)
4. In the background: cache is cleared → BigQuery queried → rows mapped → batched into EhCache
5. Once complete, `lastSuccessfulLoad` is set → `/health/cache` returns `200 HEALTHY`
6. Countdown timer starts ticking in the console

### Reload cycle

Every 5 minutes (after the previous load finishes):

```
cache.clear()
  → runQuery() → 1M rows streamed from BigQuery
  → mapToRecord() → each FieldValueList → MyRecord POJO
  → accumulate into Map<String, MyRecord> of 10,000 rows
  → cache.putAll(batch) → flush to EhCache
  → repeat until all rows loaded
  → lastSuccessfulLoad updated
  → nextReloadAt = now + 5 minutes
  → countdown restarts
```

### EhCache tier promotion

When a record is accessed, EhCache automatically promotes it up the tier hierarchy:

```
Disk (10GB) → Off-heap (1GB) → Heap (100k entries)
              slowest                    fastest
```

Hot records stay in heap. Cold records spill down. New records always enter at the heap tier and are evicted downward as the heap fills.

---

## Resilience

### CircuitBreaker states

| State | Meaning | Your app behaviour |
|---|---|---|
| `CLOSED` | All calls pass through normally | Normal reload every 5 minutes |
| `OPEN` | Calls rejected instantly (fail-fast) | Stale cache served, `503` on `/health/cache` |
| `HALF_OPEN` | 3 test calls allowed | Testing if BQ recovered |

### Exception type detection

Every BigQuery error is classified and logged with a specific tag:

```
BQ_ERROR [AUTH]    → credentials expired
BQ_ERROR [QUOTA]   → rate limit exceeded
BQ_ERROR [NETWORK] → connection dropped
BQ_ERROR [TIMEOUT] → socket timed out
BQ_ERROR [SERVICE] → Google-side outage
BQ_ERROR [NOT_FOUND] → wrong dataset/table name
```

### Partial load protection

If a reload returns fewer than 10% of the last known row count, the new data is rejected and the existing cache is preserved:

```
WARN: PARTIAL LOAD DETECTED: loaded 50,000 rows but last known count was 1,000,005.
      Threshold is 10%. Old cache data is preserved.
```

### Escalation alerts

After 3 consecutive failures, a structured JSON alert is logged:

```json
{
  "timestamp": "2026-04-01T15:00:00Z",
  "cbState": "OPEN",
  "consecutiveFailures": 3,
  "errorMessage": "Connection refused",
  "lastSuccessfulLoad": "2026-04-01T14:32:46Z",
  "dataAgeMinutes": "27.23",
  "lastKnownRowCount": 1000005,
  "loadWasPartial": false
}
```

---

## Observability

### Micrometer gauges (Prometheus)

```bash
curl http://localhost:8080/actuator/prometheus | grep -E "cache_size|cache_data|circuit_breaker"
```

| Metric | Description | Values |
|---|---|---|
| `cache.size` | Current number of entries in EhCache | 0 – 1,000,000+ |
| `cache.data.age.hours` | Hours since last successful load | 0.0 – N |
| `circuit.breaker.state` | Circuit breaker state | 0=CLOSED, 1=HALF_OPEN, 2=OPEN |

### Spring Boot Actuator

```bash
curl http://localhost:8080/actuator/health     # overall app health
curl http://localhost:8080/actuator/prometheus # all Prometheus metrics
```

---

## Performance

Measured on a MacBook Air with 1,000,005 rows:

| Phase | Time |
|---|---|
| BigQuery query (network + execution) | 3 – 6 seconds |
| Cache insert (EhCache `putAll`) | 40 – 60 seconds |
| Total reload cycle | ~40 – 70 seconds |
| Single record lookup from cache | < 1 millisecond |

The cache insert time is consistent across reloads because the cache is cleared before every load — writing into an empty cache is always faster than overwriting existing entries.

---


