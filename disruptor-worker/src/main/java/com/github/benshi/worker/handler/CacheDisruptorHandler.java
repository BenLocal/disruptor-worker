package com.github.benshi.worker.handler;

import java.util.concurrent.TimeUnit;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.WorkHandlerMessage;
import com.github.benshi.worker.WorkerHandlerEvent;
import com.github.benshi.worker.cache.LimitsManager;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CacheDisruptorHandler extends BaseDisruptorHandler {
    private final RedissonClient redissonClient;
    private final LimitsManager limitsManager;

    @Override
    public void onEvent(WorkerHandlerEvent event) throws Exception {
        if (event.getCtx() == null || event.getHandler() == null) {
            return;
        }

        WorkContext ctx = event.getCtx();
        try {
            RLock lock = redissonClient.getLock(ctx.lockKey());
            if (lock.isLocked()) {
                return;
            }
            if (lock.tryLock(2, 30, TimeUnit.SECONDS)) {
                try {
                    event.getHandler().run(new WorkHandlerMessage(ctx.getId(),
                            ctx.getWorkId(), ctx.getPayload()));
                    log.info("Job {} completed successfully", ctx.getId());
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception e) {
            log.error("Error processing job {} for workId {} with handler {}", ctx.getId(), ctx.getWorkId(),
                    ctx.getHandlerId(), e);
        } finally {
            if (event != null && event.getCtx() != null && event.getCtx().getHandlerId() != null) {
                // Decrement the count when job is finished (whether success or failure)
                try {
                    limitsManager.decrementCount(event.getCtx().getHandlerId());
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, WorkerHandlerEvent event) {
        if (ex instanceof java.lang.InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            log.error("Exception processing event {} with sequence {}", event, sequence, ex);
        }
    }

}
