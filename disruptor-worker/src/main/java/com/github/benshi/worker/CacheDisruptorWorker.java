package com.github.benshi.worker;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import com.github.benshi.worker.cache.LimitsManager;
import com.github.benshi.worker.cache.LocalLimitsManager;
import com.github.benshi.worker.handler.CacheDisruptorHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CacheDisruptorWorker extends BaseDisruptorWorker {
    private final LimitsManager limitsManager;

    public CacheDisruptorWorker(RedissonClient redissonClient,
            DisruptorWorkerOptions options) {
        this(redissonClient, options, new LocalLimitsManager());
    }

    public CacheDisruptorWorker(RedissonClient redissonClient,
            DisruptorWorkerOptions options,
            LimitsManager limitsManager) {
        super(redissonClient, new CacheDisruptorHandler(redissonClient, limitsManager), options);
        this.limitsManager = limitsManager;
    }

    @Override
    public boolean submit(WorkContext ctx) {
        if (ctx == null || ctx.getHandlerId() == null) {
            return false;
        }

        try {
            WorkerHandler handler = jobHandlers.get(ctx.getHandlerId());
            if (handler == null) {
                log.debug("No handler found for job {}, handlerId: {}", ctx.getId(), ctx.getHandlerId());
                return false;
            }

            // Check ring buffer capacity
            if (ringBuffer.remainingCapacity() <= 1) {
                log.warn("Ring buffer capacity too low: {}", ringBuffer.remainingCapacity());
                // Stop processing more jobs
                return false;
            }

            // Check handler limit
            Integer limit = jobLimits.get(ctx.getHandlerId());
            if (limit != null && limit > 0) {
                long currentCount = limitsManager.getCount(ctx.getHandlerId());
                if (currentCount >= limit) {
                    log.debug("Job limit reached for handler {}: {} >= {}", ctx.getHandlerId(), currentCount,
                            limit);
                    return false;
                }
            }

            RLock lock = redissonClient.getLock(ctx.lockKey());
            if (lock.isLocked() && !lock.isHeldByCurrentThread()) {
                // Job is still running, skip
                return false;
            }

            // Add to ring buffer for processing
            long sequence = ringBuffer.tryNext();
            if (sequence < 0) {
                log.warn("Failed to get sequence from ring buffer");
                return false;
            }
            try {
                // Increment the count
                limitsManager.incrementCount(ctx.getHandlerId());

                WorkerHandlerEvent event = ringBuffer.get(sequence);
                event.setCtx(ctx);
                event.setHandler(handler);
                log.debug("Job {} added to ring buffer for processing", ctx.getId());
            } finally {
                ringBuffer.publish(sequence);
            }
        } catch (Exception e) {
            log.error("Error submitting job {} to ring buffer", ctx.getId(), e);
            return false;
        }

        return true;
    }

}
