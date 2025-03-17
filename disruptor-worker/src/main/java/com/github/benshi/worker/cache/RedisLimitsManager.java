package com.github.benshi.worker.cache;

import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;

public class RedisLimitsManager implements LimitsManager {
    private final RedissonClient redissonClient;
    private static final String HANDLER_COUNT_KEY = "job:handler:count:";
    private static final int DEFAULT_EXPIRY_SECONDS = 24 * 60 * 60; // 24 hours

    public RedisLimitsManager(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Override
    public long incrementCount(String handlerId) {
        RAtomicLong counter = redissonClient.getAtomicLong(HANDLER_COUNT_KEY + handlerId);
        long value = counter.incrementAndGet();
        counter.expire(java.time.Duration.ofSeconds(DEFAULT_EXPIRY_SECONDS));
        return value;
    }

    @Override
    public long decrementCount(String handlerId) {
        RAtomicLong counter = redissonClient.getAtomicLong(HANDLER_COUNT_KEY + handlerId);
        long value = counter.decrementAndGet();
        if (value <= 0) {
            counter.delete();
            return 0;
        }
        return value;
    }

    @Override
    public long getCount(String handlerId) {
        RAtomicLong counter = redissonClient.getAtomicLong(HANDLER_COUNT_KEY + handlerId);
        return counter.get();
    }
}
