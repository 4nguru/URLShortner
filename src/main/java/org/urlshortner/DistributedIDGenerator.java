package org.urlshortner;

public class DistributedIDGenerator {
    private final long serverId;
    private final long rangeSize;
    private final RedisClient redis;
    
    private long currentId;
    private long maxId;
    private boolean rangeExhausted = true;
    
    public DistributedIDGenerator(long serverId, long rangeSize, RedisClient redis) {
        this.serverId = serverId;
        this.rangeSize = rangeSize;
        this.redis = redis;
    }
    
    public synchronized long getNextId() {
        if (rangeExhausted || currentId >= maxId) {
            allocateNewRange();
        }
        return ++currentId;
    }
    
    private void allocateNewRange() {
        // Atomically get next range from Redis
        String key = "id_range_allocator";
        long rangeStart = redis.incrBy(key, rangeSize) - rangeSize + 1;
        
        this.currentId = rangeStart - 1;
        this.maxId = rangeStart + rangeSize - 1;
        this.rangeExhausted = false;
        
        System.out.println("Server " + serverId + " allocated range: " + rangeStart + " to " + maxId);
    }
    
    public long getRemainingIds() {
        return rangeExhausted ? 0 : maxId - currentId;
    }
    
    // Simple Redis client interface
    public interface RedisClient {
        long incrBy(String key, long increment);
    }
}
