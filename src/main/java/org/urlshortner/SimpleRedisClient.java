package org.urlshortner;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleRedisClient implements DistributedIDGenerator.RedisClient {
    private final ConcurrentHashMap<String, AtomicLong> store = new ConcurrentHashMap<>();
    
    @Override
    public long incrBy(String key, long increment) {
        return store.computeIfAbsent(key, k -> new AtomicLong(0))
                   .addAndGet(increment);
    }
    
    public long get(String key) {
        AtomicLong value = store.get(key);
        return value != null ? value.get() : 0;
    }
}