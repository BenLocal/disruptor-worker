package com.github.benshi.worker.cache;

import java.util.concurrent.ConcurrentHashMap;

public class LocalLimitsManager implements LimitsManager {
    private static final ConcurrentHashMap<String, Long> handlerCount = new ConcurrentHashMap<>();

    @Override
    public long incrementCount(String handlerId) {
        return handlerCount.compute(handlerId, (k, v) -> v == null ? 1 : v + 1);
    }

    @Override
    public long decrementCount(String handlerId) {
        return handlerCount.compute(handlerId, (k, v) -> v == null ? 0 : v - 1);
    }

    @Override
    public long getCount(String handlerId) {
        return handlerCount.getOrDefault(handlerId, 0L);
    }

}
