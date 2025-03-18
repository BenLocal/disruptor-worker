package com.github.benshi.worker.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalLimitsManager implements LimitsManager {
    private static final ConcurrentHashMap<String, AtomicInteger> handlerCount = new ConcurrentHashMap<>();

    @Override
    public long incrementCount(String handlerId) {
        if (handlerId == null) {
            return -1;
        }

        return handlerCount.compute(handlerId, (k, v) -> {
            if (v == null) {
                return new AtomicInteger(1);
            } else {
                v.incrementAndGet();
                return v;
            }
        }).get();
    }

    @Override
    public long decrementCount(String handlerId) {
        if (handlerId == null) {
            return -1;
        }

        return handlerCount.compute(handlerId, (k, v) -> {
            if (v == null) {
                return new AtomicInteger(0);
            } else {
                v.decrementAndGet();
                return v;
            }
        }).get();
    }

    @Override
    public long getCount(String handlerId) {
        if (handlerId == null) {
            return -1;
        }

        AtomicInteger v = handlerCount.get(handlerId);
        if (v == null) {
            return 0;
        }

        return v.get();
    }

}
