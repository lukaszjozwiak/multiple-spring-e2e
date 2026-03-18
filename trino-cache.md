**User:**
I am java programmer and have spring boot 3 aplication. Part of the flow is to retrive 
Resultser of long running trino query. Based on that resultset, lookup table need to be build in app. I need to cache query result, I also want to refresh cache every day at night or on demand. Please provide code example. Use spring features when possible.

---

**AI:**
A clean Spring Boot 3 approach is:


- load the Trino result into an in-memory lookup structure
- expose it through a service
- refresh it on startup, nightly, and on demand
- keep reads lock-free or nearly lock-free


For this use case, a good pattern is:


- `JdbcTemplate` or `NamedParameterJdbcTemplate` for Trino access
- `@EnableScheduling` + `@Scheduled` for nightly refresh
- `AtomicReference<Map<...>>` for safe cache replacement
- optional `@RestController` endpoint for manual refresh


This is usually better than Spring Cache abstraction alone, because you are not caching individual method calls by key. You are building and replacing a whole lookup table snapshot.


---


## Recommended design


Assume your Trino query returns rows like:


- `customer_id`
- `country`
- `segment`


and you want a lookup by `customer_id`.


At refresh time:


1. execute query
2. transform rows into `Map<String, CustomerLookup>`
3. atomically replace current cache with the new snapshot


Readers always see a fully built map, never a partially updated one.


---


## 1. Configuration


### Main application


```Java
package com.example.trinocache;import org.springframework.boot.SpringApplication;import org.springframework.boot.autoconfigure.SpringBootApplication;import org.springframework.scheduling.annotation.EnableScheduling;@EnableScheduling@SpringBootApplicationpublic class TrinoCacheApplication {    public static void main(String[] args) {        SpringApplication.run(TrinoCacheApplication.class, args);    }}
```


---


## 2. Properties


Use typed configuration for SQL and schedule.


```Java
package com.example.trinocache.config;import org.springframework.boot.context.properties.ConfigurationProperties;@ConfigurationProperties(prefix = "app.lookup-cache")public class LookupCacheProperties {    /**     * Cron for nightly refresh.     * Example: 0 0 2 * * *  => every day at 02:00     */    private String refreshCron = "0 0 2 * * *";    /**     * Trino query used to build the lookup cache.     */    private String query;    public String getRefreshCron() {        return refreshCron;    }    public void setRefreshCron(String refreshCron) {        this.refreshCron = refreshCron;    }    public String getQuery() {        return query;    }    public void setQuery(String query) {        this.query = query;    }}
```


Register it:


```Java
package com.example.trinocache.config;import org.springframework.boot.context.properties.EnableConfigurationProperties;import org.springframework.context.annotation.Configuration;@Configuration@EnableConfigurationProperties(LookupCacheProperties.class)public class LookupCacheConfig {}
```


---


## 3. Data source configuration for Trino


If Trino is your main datasource, standard Spring Boot datasource config may be enough.


Example `application.yml`:


```YAML
spring:  datasource:    url: jdbc:trino://trino-host:8080/my_catalog/my_schema    username: my_user    password: my_password    driver-class-name: io.trino.jdbc.TrinoDriverapp:  lookup-cache:    refresh-cron: "0 0 2 * * *"    query: |      SELECT customer_id, country, segment      FROM customer_lookup_source
```


If you already have a custom Trino datasource bean, that is also fine.


---


## 4. DTO representing one lookup row


```Java
package com.example.trinocache.lookup;public record CustomerLookup(        String customerId,        String country,        String segment) {}
```


---


## 5. Repository for loading data from Trino


Use `JdbcTemplate`.


```Java
package com.example.trinocache.lookup;import com.example.trinocache.config.LookupCacheProperties;import org.springframework.jdbc.core.JdbcTemplate;import org.springframework.stereotype.Repository;import java.util.List;@Repositorypublic class CustomerLookupRepository {    private final JdbcTemplate jdbcTemplate;    private final LookupCacheProperties properties;    public CustomerLookupRepository(JdbcTemplate jdbcTemplate, LookupCacheProperties properties) {        this.jdbcTemplate = jdbcTemplate;        this.properties = properties;    }    public List<CustomerLookup> loadAll() {        return jdbcTemplate.query(                properties.getQuery(),                (rs, rowNum) -> new CustomerLookup(                        rs.getString("customer_id"),                        rs.getString("country"),                        rs.getString("segment")                )        );    }}
```


---


## 6. Cache service


This is the core.


### Key points


- `AtomicReference<Map<...>>` stores the current immutable snapshot
- refresh builds a new map first, then swaps it in one step
- `@PostConstruct` loads initial state
- `@Scheduled` performs nightly refresh
- synchronized refresh prevents overlapping refresh executions


```Java
package com.example.trinocache.lookup;import com.example.trinocache.config.LookupCacheProperties;import jakarta.annotation.PostConstruct;import org.slf4j.Logger;import org.slf4j.LoggerFactory;import org.springframework.scheduling.annotation.Scheduled;import org.springframework.stereotype.Service;import java.time.Instant;import java.util.List;import java.util.Map;import java.util.Optional;import java.util.concurrent.atomic.AtomicReference;import java.util.function.Function;import java.util.stream.Collectors;@Servicepublic class CustomerLookupCacheService {    private static final Logger log = LoggerFactory.getLogger(CustomerLookupCacheService.class);    private final CustomerLookupRepository repository;    private final LookupCacheProperties properties;    private final AtomicReference<Map<String, CustomerLookup>> cacheRef =            new AtomicReference<>(Map.of());    private final AtomicReference<Instant> lastRefreshRef =            new AtomicReference<>(null);    public CustomerLookupCacheService(CustomerLookupRepository repository,                                      LookupCacheProperties properties) {        this.repository = repository;        this.properties = properties;    }    @PostConstruct    public void init() {        refreshCache();    }    public Optional<CustomerLookup> findByCustomerId(String customerId) {        return Optional.ofNullable(cacheRef.get().get(customerId));    }    public Map<String, CustomerLookup> getSnapshot() {        return cacheRef.get();    }    public Instant getLastRefreshTime() {        return lastRefreshRef.get();    }    @Scheduled(cron = "${app.lookup-cache.refresh-cron}")    public void scheduledRefresh() {        refreshCache();    }    public synchronized void refreshCache() {        log.info("Refreshing customer lookup cache...");        long start = System.currentTimeMillis();        List<CustomerLookup> rows = repository.loadAll();        Map<String, CustomerLookup> newCache = rows.stream()                .collect(Collectors.toUnmodifiableMap(                        CustomerLookup::customerId,                        Function.identity(),                        (existing, replacement) -> replacement                ));        cacheRef.set(newCache);        lastRefreshRef.set(Instant.now());        long durationMs = System.currentTimeMillis() - start;        log.info("Customer lookup cache refreshed. Rows={}, durationMs={}", newCache.size(), durationMs);    }}
```


---


## 7. On-demand refresh endpoint


```Java
package com.example.trinocache.lookup;import org.springframework.http.ResponseEntity;import org.springframework.web.bind.annotation.*;import java.time.Instant;import java.util.Map;@RestController@RequestMapping("/api/lookup-cache")public class CustomerLookupCacheController {    private final CustomerLookupCacheService cacheService;    public CustomerLookupCacheController(CustomerLookupCacheService cacheService) {        this.cacheService = cacheService;    }    @PostMapping("/refresh")    public ResponseEntity<Map<String, Object>> refresh() {        cacheService.refreshCache();        return ResponseEntity.ok(Map.of(                "status", "OK",                "size", cacheService.getSnapshot().size(),                "lastRefreshTime", cacheService.getLastRefreshTime()        ));    }    @GetMapping("/status")    public ResponseEntity<Map<String, Object>> status() {        Instant lastRefreshTime = cacheService.getLastRefreshTime();        return ResponseEntity.ok(Map.of(                "size", cacheService.getSnapshot().size(),                "lastRefreshTime", lastRefreshTime        ));    }    @GetMapping("/{customerId}")    public ResponseEntity<CustomerLookup> getByCustomerId(@PathVariable String customerId) {        return cacheService.findByCustomerId(customerId)                .map(ResponseEntity::ok)                .orElseGet(() -> ResponseEntity.notFound().build());    }}
```


---


## 8. Example usage from business flow


```Java
package com.example.trinocache.business;import com.example.trinocache.lookup.CustomerLookup;import com.example.trinocache.lookup.CustomerLookupCacheService;import org.springframework.stereotype.Service;@Servicepublic class PricingService {    private final CustomerLookupCacheService lookupCacheService;    public PricingService(CustomerLookupCacheService lookupCacheService) {        this.lookupCacheService = lookupCacheService;    }    public String resolveSegment(String customerId) {        return lookupCacheService.findByCustomerId(customerId)                .map(CustomerLookup::segment)                .orElse("UNKNOWN");    }}
```


---


## Why this pattern works well


### Atomic snapshot replacement


Instead of mutating a shared `HashMap`, you:


- build new map separately
- replace old map in one operation


That avoids inconsistent intermediate state.


### Good read performance


Reads are very fast:


```Java
cacheRef.get().get(key)
```


No heavy locking on lookup path.


### Controlled refresh


Nightly refresh and manual refresh reuse the same method.


---


## What about Spring @Cacheable?


You could use Spring Cache, but it is better for this kind of scenario:


- method result cached by arguments
- for example: `findCustomer(id)`


Your scenario is different:


- one long-running query
- whole resultset materialized into app memory
- lookup table built from that resultset
- full refresh on schedule


So a dedicated in-memory snapshot service is simpler and more explicit.


---


## Better variant for production: separate scheduler from refresh logic


Sometimes it is cleaner to keep scheduling outside the cache service.


```Java
package com.example.trinocache.lookup;import org.springframework.scheduling.annotation.Scheduled;import org.springframework.stereotype.Component;@Componentpublic class CustomerLookupCacheScheduler {    private final CustomerLookupCacheService cacheService;    public CustomerLookupCacheScheduler(CustomerLookupCacheService cacheService) {        this.cacheService = cacheService;    }    @Scheduled(cron = "${app.lookup-cache.refresh-cron}")    public void scheduledRefresh() {        cacheService.refreshCache();    }}
```


Then remove `@Scheduled` from the service itself.


This gives slightly better separation of concerns.


---


## Important production concerns


### 1. What if refresh fails?


You usually do **not** want to clear the cache if the Trino query fails.


The current example already behaves correctly:


- new cache is built first
- only if query succeeds, cache is replaced


So old cache remains active on failure.


You should add logging and maybe metrics:


```Java
public synchronized void refreshCache() {    log.info("Refreshing customer lookup cache...");    try {        List<CustomerLookup> rows = repository.loadAll();        Map<String, CustomerLookup> newCache = rows.stream()                .collect(Collectors.toUnmodifiableMap(                        CustomerLookup::customerId,                        Function.identity(),                        (a, b) -> b                ));        cacheRef.set(newCache);        lastRefreshRef.set(Instant.now());        log.info("Refresh successful. Rows={}", newCache.size());    } catch (Exception e) {        log.error("Refresh failed. Keeping previous cache snapshot.", e);    }}
```


### 2. Startup behavior


Decide whether application should fail if initial load fails.


Two common options:


- **strict**: fail startup if cache cannot be built
- **lenient**: start with empty cache and retry later


For critical lookup data, strict is often better.


### 3. Large result sets


If resultset is large, watch heap usage. A full in-memory map can be expensive.


### 4. Duplicate keys


If query can return duplicate keys, decide merge policy explicitly.


Current code uses:


```Java
(existing, replacement) -> replacement
```


meaning last row wins.


### 5. Multi-node deployment


If your app runs multiple instances, each instance will maintain its own cache. That is often fine. If not, you need distributed caching or externalized refresh orchestration.


---


## Generic reusable version


If you want a more reusable cache component, you can abstract the snapshot pattern.


### Generic snapshot cache helper


```Java
package com.example.trinocache.common;import java.time.Instant;import java.util.Map;import java.util.Optional;import java.util.concurrent.atomic.AtomicReference;public class SnapshotCache<K, V> {    private final AtomicReference<Map<K, V>> cacheRef = new AtomicReference<>(Map.of());    private final AtomicReference<Instant> lastRefreshRef = new AtomicReference<>(null);    public Optional<V> get(K key) {        return Optional.ofNullable(cacheRef.get().get(key));    }    public Map<K, V> snapshot() {        return cacheRef.get();    }    public Instant lastRefreshTime() {        return lastRefreshRef.get();    }    public void replace(Map<K, V> newSnapshot) {        cacheRef.set(Map.copyOf(newSnapshot));        lastRefreshRef.set(Instant.now());    }}
```


Then your service just loads and replaces.


---


## Alternative: Caffeine


If you want richer local cache features, use Caffeine. But for a full snapshot refreshed nightly, Caffeine is not necessary unless you specifically need:


- size limits
- expiry policies
- stats
- async loading


For your use case, plain immutable snapshot replacement is usually enough.


---


## Minimal version with Caffeine-backed holder


If you specifically want a Spring cache provider, Caffeine is the usual choice. But I would still keep the snapshot approach and only store the whole lookup map as one cached object, not cache each row independently.


---


## Final recommendation


For your case, use:


- `JdbcTemplate` to query Trino
- `AtomicReference<Map<K,V>>` as in-memory lookup snapshot
- `@PostConstruct` for initial load
- `@Scheduled` for nightly refresh
- REST/admin method for manual refresh


That is simple, thread-safe, and idiomatic in Spring Boot.


I can also provide a second version using:


- `@ConfigurationProperties` + separate Trino datasource bean
- `NamedParameterJdbcTemplate`
- Actuator custom endpoint instead of REST controller
- generic reusable cache service for multiple lookup tables.

---

**User:**
Please explain how about solution guards against cache replacement in case of query failure.  I didn't grasp that?

---

**AI:**
The protection comes from the **order of operations**.


The code does **not** do this:


```Java
cacheRef.set(Map.of()); // clear current cacheList<CustomerLookup> rows = repository.loadAll(); // query TrinocacheRef.set(buildMap(rows));
```


That would be dangerous, because if `loadAll()` failed, your cache would already be empty.


Instead, it does this:


```Java
List<CustomerLookup> rows = repository.loadAll();   // 1. query firstMap<String, CustomerLookup> newCache = rows.stream()        .collect(...);                              // 2. build new map separatelycacheRef.set(newCache);                             // 3. replace old cache only at the end
```


So there are two distinct objects:


- **old cache** â€” currently used by the application
- **new cache** â€” temporary map being built during refresh


Until `cacheRef.set(newCache)` happens, the application still uses the old cache.


---


## What happens when query fails


Assume current cache contains:


```
Java{ "A" -> ..., "B" -> ... }
```


Now refresh starts.


### Step 1


Code calls:


```Java
List<CustomerLookup> rows = repository.loadAll();
```


If Trino throws exception here:


```Java
throw new RuntimeException("Trino timeout");
```


then execution never reaches:


```Java
cacheRef.set(newCache);
```


So `cacheRef` still points to the previous map:


```Java
{ "A" -> ..., "B" -> ... }
```


Nothing was replaced.


---


## What happens when map building fails


Even if query succeeds, map creation could still fail, for example because of duplicate keys when using a collector without merge function.


Example:


```Java
Map<String, CustomerLookup> newCache = rows.stream()        .collect(Collectors.toUnmodifiableMap(                CustomerLookup::customerId,                Function.identity()        ));
```


If duplicate `customerId` appears, this line throws exception.


Again, `cacheRef.set(newCache)` is never executed.


So old cache remains active.


---


## Why AtomicReference helps


`AtomicReference` is important because replacement is a **single atomic pointer swap**:


```
JavacacheRef.set(newCache);
```


Readers either see:


- the entire old map, or
- the entire new map


They never see a half-built map.


That is why this pattern is often called **snapshot replacement**.


---


## Visual picture


### Before refresh


```Java
cacheRef ---> OLD_MAP
```


### During refresh


```Java
cacheRef ---> OLD_MAPnewCache ---> building in local variable
```


If anything fails here, `cacheRef` still points to `OLD_MAP`.


### After successful refresh


```Java
cacheRef ---> NEW_MAP
```


Only then does the application start using the new data.


---


## Why local variable matters


This line is the key idea:


```Java
Map<String, CustomerLookup> newCache = ...
```


`newCache` is just a local variable. It is not visible to the rest of the application until you assign it into `cacheRef`.


So failures while creating `newCache` do not corrupt the currently active cache.


---


## Contrast with unsafe approach


Unsafe version:


```Java
public synchronized void refreshCache() {    Map<String, CustomerLookup> cache = cacheRef.get();    cache.clear(); // dangerous    List<CustomerLookup> rows = repository.loadAll(); // may fail    for (CustomerLookup row : rows) {        cache.put(row.customerId(), row);    }}
```


Problem:


- existing cache object is modified in place
- if query fails after `clear()`, cache becomes empty
- if failure happens during `put()`, cache becomes partially filled
- readers may observe inconsistent state


That is exactly what the snapshot replacement approach avoids.


---


## Safer refresh version with explicit try/catch


This makes the behavior more obvious:


```Java
public synchronized void refreshCache() {    log.info("Refreshing customer lookup cache...");    try {        List<CustomerLookup> rows = repository.loadAll();        Map<String, CustomerLookup> newCache = rows.stream()                .collect(Collectors.toUnmodifiableMap(                        CustomerLookup::customerId,                        Function.identity(),                        (existing, replacement) -> replacement                ));        cacheRef.set(newCache);        lastRefreshRef.set(Instant.now());        log.info("Refresh successful. Rows={}", newCache.size());    } catch (Exception e) {        log.error("Refresh failed. Previous cache snapshot is kept.", e);    }}
```


Behavior:


- query/build fails â†’ log error, keep old cache
- query/build succeeds â†’ replace cache


---


## One subtle point


This protects against **replacement with bad/empty state due to refresh failure**.


It does **not** automatically protect against a logically bad but technically successful query, for example:


- query returns 0 rows because upstream source is broken
- query succeeds but data is invalid


In that case, refresh still succeeds and cache gets replaced with bad data.


If that matters, add validation before swap, for example:


```Java
if (newCache.isEmpty()) {    throw new IllegalStateException("Refusing to replace cache with empty snapshot");}
```


or:


```Java
if (newCache.size() < minimumExpectedSize) {    throw new IllegalStateException("Snapshot too small, likely invalid");}
```


Then only validated snapshots are published.


---


## In one sentence


The cache is protected because the code builds the new snapshot completely off to the side, and only swaps it into the live reference after the whole refresh has succeeded.

---

**User:**
Add retry option to refresh cache method, or in place you consider suitable

---

**AI:**
Yes. The cleanest place is usually the **refresh operation itself**, not the scheduler.


Reason:


- scheduled refresh
- startup refresh
- manual refresh


all should get the same retry behavior.


So instead of retrying only `@Scheduled`, make the underlying refresh method retryable.


---


## Recommended approach: Spring Retry


Use Spring Retry with:


- `@Retryable` on refresh method
- `@Recover` for final failure handling
- `@EnableRetry` in configuration


This is the most Spring-native solution.


---


## 1. Dependency


For Maven:


```XML
<dependency>    <groupId>org.springframework.retry</groupId>    <artifactId>spring-retry</artifactId></dependency><dependency>    <groupId>org.springframework</groupId>    <artifactId>spring-aspects</artifactId></dependency>
```


---


## 2. Enable retry


```Java
package com.example.trinocache;import org.springframework.boot.SpringApplication;import org.springframework.boot.autoconfigure.SpringBootApplication;import org.springframework.retry.annotation.EnableRetry;import org.springframework.scheduling.annotation.EnableScheduling;@EnableRetry@EnableScheduling@SpringBootApplicationpublic class TrinoCacheApplication {    public static void main(String[] args) {        SpringApplication.run(TrinoCacheApplication.class, args);    }}
```


---


## 3. Important proxy note


`@Retryable` works through a Spring proxy.


That means this usually **will not work correctly** if a method inside the same bean calls another `@Retryable` method directly through `this`.


So instead of:


- `scheduledRefresh()` in same class calling `refreshCache()`


it is better to split responsibilities:


- one bean = actual refresh logic with `@Retryable`
- another bean = scheduler / controller using that bean


That is the safest layout.


---


## 4. Retryable refresher bean


```Java
package com.example.trinocache.lookup;import org.slf4j.Logger;import org.slf4j.LoggerFactory;import org.springframework.retry.annotation.Backoff;import org.springframework.retry.annotation.Recover;import org.springframework.retry.annotation.Retryable;import org.springframework.stereotype.Component;import java.time.Instant;import java.util.List;import java.util.Map;import java.util.concurrent.atomic.AtomicReference;import java.util.function.Function;import java.util.stream.Collectors;@Componentpublic class CustomerLookupCacheRefresher {    private static final Logger log = LoggerFactory.getLogger(CustomerLookupCacheRefresher.class);    private final CustomerLookupRepository repository;    private final AtomicReference<Map<String, CustomerLookup>> cacheRef;    private final AtomicReference<Instant> lastRefreshRef;    public CustomerLookupCacheRefresher(CustomerLookupRepository repository,                                        CustomerLookupCacheState cacheState) {        this.repository = repository;        this.cacheRef = cacheState.cacheRef();        this.lastRefreshRef = cacheState.lastRefreshRef();    }    @Retryable(            retryFor = {Exception.class},            maxAttempts = 3,            backoff = @Backoff(delay = 5000, multiplier = 2.0)    )    public void refreshCache() {        log.info("Refreshing customer lookup cache...");        long start = System.currentTimeMillis();        List<CustomerLookup> rows = repository.loadAll();        Map<String, CustomerLookup> newCache = rows.stream()                .collect(Collectors.toUnmodifiableMap(                        CustomerLookup::customerId,                        Function.identity(),                        (existing, replacement) -> replacement                ));        validateSnapshot(newCache);        cacheRef.set(newCache);        lastRefreshRef.set(Instant.now());        long durationMs = System.currentTimeMillis() - start;        log.info("Customer lookup cache refreshed. Rows={}, durationMs={}", newCache.size(), durationMs);    }    private void validateSnapshot(Map<String, CustomerLookup> newCache) {        if (newCache.isEmpty()) {            throw new IllegalStateException("Refusing to replace cache with empty snapshot");        }    }    @Recover    public void recover(Exception ex) {        log.error("Cache refresh failed after all retry attempts. Previous cache snapshot is kept.", ex);    }}
```


---


## 5. Separate cache state holder


This keeps shared state in one bean.


```Java
package com.example.trinocache.lookup;import org.springframework.stereotype.Component;import java.time.Instant;import java.util.Map;import java.util.concurrent.atomic.AtomicReference;@Componentpublic class CustomerLookupCacheState {    private final AtomicReference<Map<String, CustomerLookup>> cacheRef =            new AtomicReference<>(Map.of());    private final AtomicReference<Instant> lastRefreshRef =            new AtomicReference<>(null);    public AtomicReference<Map<String, CustomerLookup>> cacheRef() {        return cacheRef;    }    public AtomicReference<Instant> lastRefreshRef() {        return lastRefreshRef;    }}
```


---


## 6. Service for reads


```Java
package com.example.trinocache.lookup;import org.springframework.stereotype.Service;import java.time.Instant;import java.util.Map;import java.util.Optional;@Servicepublic class CustomerLookupCacheService {    private final CustomerLookupCacheState cacheState;    public CustomerLookupCacheService(CustomerLookupCacheState cacheState) {        this.cacheState = cacheState;    }    public Optional<CustomerLookup> findByCustomerId(String customerId) {        return Optional.ofNullable(cacheState.cacheRef().get().get(customerId));    }    public Map<String, CustomerLookup> getSnapshot() {        return cacheState.cacheRef().get();    }    public Instant getLastRefreshTime() {        return cacheState.lastRefreshRef().get();    }}
```


---


## 7. Scheduler bean


```Java
package com.example.trinocache.lookup;import jakarta.annotation.PostConstruct;import org.springframework.scheduling.annotation.Scheduled;import org.springframework.stereotype.Component;@Componentpublic class CustomerLookupCacheScheduler {    private final CustomerLookupCacheRefresher refresher;    public CustomerLookupCacheScheduler(CustomerLookupCacheRefresher refresher) {        this.refresher = refresher;    }    @PostConstruct    public void init() {        refresher.refreshCache();    }    @Scheduled(cron = "${app.lookup-cache.refresh-cron}")    public void scheduledRefresh() {        refresher.refreshCache();    }}
```


---


## 8. Manual refresh endpoint


```Java
package com.example.trinocache.lookup;import org.springframework.http.ResponseEntity;import org.springframework.web.bind.annotation.*;import java.util.Map;@RestController@RequestMapping("/api/lookup-cache")public class CustomerLookupCacheController {    private final CustomerLookupCacheRefresher refresher;    private final CustomerLookupCacheService cacheService;    public CustomerLookupCacheController(CustomerLookupCacheRefresher refresher,                                         CustomerLookupCacheService cacheService) {        this.refresher = refresher;        this.cacheService = cacheService;    }    @PostMapping("/refresh")    public ResponseEntity<Map<String, Object>> refresh() {        refresher.refreshCache();        return ResponseEntity.ok(Map.of(                "status", "OK",                "size", cacheService.getSnapshot().size(),                "lastRefreshTime", cacheService.getLastRefreshTime()        ));    }    @GetMapping("/status")    public ResponseEntity<Map<String, Object>> status() {        return ResponseEntity.ok(Map.of(                "size", cacheService.getSnapshot().size(),                "lastRefreshTime", cacheService.getLastRefreshTime()        ));    }}
```


---


## What happens on retry


Suppose `repository.loadAll()` fails with timeout.


Attempt 1:


- query fails
- no `cacheRef.set(...)`
- old cache remains


Attempt 2:


- query fails again
- old cache remains


Attempt 3:


- query succeeds
- new snapshot validated
- `cacheRef.set(newCache)` executed


If all attempts fail:


- `@Recover` runs
- old cache still remains


So retry does not weaken the safe snapshot behavior.


---


## A practical improvement: retry only transient failures


Retrying every `Exception` is often too broad.


Better to retry only transient problems, for example:


- connection failures
- temporary network failures
- timeout exceptions


and not retry obvious permanent problems such as:


- SQL syntax error
- wrong column name
- bad mapping logic


Example:


```Java
@Retryable(        retryFor = {                java.sql.SQLTransientException.class,                org.springframework.dao.TransientDataAccessException.class        },        maxAttempts = 3,        backoff = @Backoff(delay = 5000, multiplier = 2.0))public void refreshCache() {    ...}
```


This is usually better than `Exception.class`.


If the Trino JDBC driver wraps exceptions in a specific type, you can target that type too.


---


## Another practical improvement: externalize retry settings


Instead of hardcoding retry values:


```
JavamaxAttempts = 3,backoff = @Backoff(delay = 5000, multiplier = 2.0)
```


you can make them configurable:


```Java
@Retryable(        retryFor = {org.springframework.dao.TransientDataAccessException.class},        maxAttemptsExpression = "${app.lookup-cache.retry.max-attempts:3}",        backoff = @Backoff(                delayExpression = "${app.lookup-cache.retry.delay-ms:5000}",                multiplierExpression = "${app.lookup-cache.retry.multiplier:2.0}"        ))public void refreshCache() {    ...}
```


And in `application.yml`:


```YAML
app:  lookup-cache:    refresh-cron: "0 0 2 * * *"    retry:      max-attempts: 3      delay-ms: 5000      multiplier: 2.0
```


That is usually the better production version.


---


## One more important point: overlapping refreshes


If manual refresh and scheduled refresh happen close together, you may want to prevent concurrent refresh execution.


`@Retryable` does not do that by itself.


You can still keep synchronization or locking around the actual refresh operation.


Example with `ReentrantLock`:


```Java
package com.example.trinocache.lookup;import org.slf4j.Logger;import org.slf4j.LoggerFactory;import org.springframework.retry.annotation.Backoff;import org.springframework.retry.annotation.Recover;import org.springframework.retry.annotation.Retryable;import org.springframework.stereotype.Component;import java.time.Instant;import java.util.List;import java.util.Map;import java.util.concurrent.atomic.AtomicReference;import java.util.concurrent.locks.ReentrantLock;import java.util.function.Function;import java.util.stream.Collectors;@Componentpublic class CustomerLookupCacheRefresher {    private static final Logger log = LoggerFactory.getLogger(CustomerLookupCacheRefresher.class);    private final CustomerLookupRepository repository;    private final AtomicReference<Map<String, CustomerLookup>> cacheRef;    private final AtomicReference<Instant> lastRefreshRef;    private final ReentrantLock refreshLock = new ReentrantLock();    public CustomerLookupCacheRefresher(CustomerLookupRepository repository,                                        CustomerLookupCacheState cacheState) {        this.repository = repository;        this.cacheRef = cacheState.cacheRef();        this.lastRefreshRef = cacheState.lastRefreshRef();    }    @Retryable(            retryFor = {org.springframework.dao.TransientDataAccessException.class},            maxAttemptsExpression = "${app.lookup-cache.retry.max-attempts:3}",            backoff = @Backoff(                    delayExpression = "${app.lookup-cache.retry.delay-ms:5000}",                    multiplierExpression = "${app.lookup-cache.retry.multiplier:2.0}"            )    )    public void refreshCache() {        if (!refreshLock.tryLock()) {            log.info("Cache refresh skipped because another refresh is already in progress.");            return;        }        try {            log.info("Refreshing customer lookup cache...");            List<CustomerLookup> rows = repository.loadAll();            Map<String, CustomerLookup> newCache = rows.stream()                    .collect(Collectors.toUnmodifiableMap(                            CustomerLookup::customerId,                            Function.identity(),                            (existing, replacement) -> replacement                    ));            validateSnapshot(newCache);            cacheRef.set(newCache);            lastRefreshRef.set(Instant.now());            log.info("Customer lookup cache refreshed. Rows={}", newCache.size());        } finally {            refreshLock.unlock();        }    }    private void validateSnapshot(Map<String, CustomerLookup> newCache) {        if (newCache.isEmpty()) {            throw new IllegalStateException("Refusing to replace cache with empty snapshot");        }    }    @Recover    public void recover(org.springframework.dao.TransientDataAccessException ex) {        log.error("Cache refresh failed after all retry attempts. Previous cache snapshot is kept.", ex);    }}
```


This prevents parallel refresh execution.


---


## My recommendation


For your case, I would use:


- snapshot replacement with `AtomicReference`
- `@Retryable` on dedicated refresh bean
- retry only transient DB/query exceptions
- `@Recover` for final logging
- validation before publish
- lock to prevent concurrent refreshes


That gives you:


- safe cache reads
- safe failure behavior
- automatic retry on temporary Trino issues
- one place for all refresh entry points


Below is the most compact version worth using.


---


## Compact final version


```Java
@Componentpublic class CustomerLookupCacheRefresher {    private static final Logger log = LoggerFactory.getLogger(CustomerLookupCacheRefresher.class);    private final CustomerLookupRepository repository;    private final CustomerLookupCacheState cacheState;    private final ReentrantLock refreshLock = new ReentrantLock();    public CustomerLookupCacheRefresher(CustomerLookupRepository repository,                                        CustomerLookupCacheState cacheState) {        this.repository = repository;        this.cacheState = cacheState;    }    @Retryable(            retryFor = {                    org.springframework.dao.TransientDataAccessException.class,                    java.sql.SQLTransientException.class            },            maxAttemptsExpression = "${app.lookup-cache.retry.max-attempts:3}",            backoff = @Backoff(                    delayExpression = "${app.lookup-cache.retry.delay-ms:5000}",                    multiplierExpression = "${app.lookup-cache.retry.multiplier:2.0}"            )    )    public void refreshCache() {        if (!refreshLock.tryLock()) {            log.info("Refresh already in progress, skipping.");            return;        }        try {            List<CustomerLookup> rows = repository.loadAll();            Map<String, CustomerLookup> newCache = rows.stream()                    .collect(Collectors.toUnmodifiableMap(                            CustomerLookup::customerId,                            Function.identity(),                            (a, b) -> b                    ));            if (newCache.isEmpty()) {                throw new IllegalStateException("Refusing to publish empty cache");            }            cacheState.cacheRef().set(newCache);            cacheState.lastRefreshRef().set(Instant.now());            log.info("Lookup cache refreshed, size={}", newCache.size());        } finally {            refreshLock.unlock();        }    }    @Recover    public void recover(Exception ex) {        log.error("Lookup cache refresh failed after retries. Previous snapshot remains active.", ex);    }}
```


And config:


```YAML
app:  lookup-cache:    refresh-cron: "0 0 2 * * *"    retry:      max-attempts: 3      delay-ms: 5000      multiplier: 2.0
```


One more thing: if you want, I can now provide a **full Spring Boot 3 example with Maven dependencies, config, Trino datasource, retry, scheduler, actuator-style manual refresh, and a generic cache abstraction**.

---

