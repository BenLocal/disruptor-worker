package com.github.benshi.worker.handler;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.WorkHandlerMessage;
import com.github.benshi.worker.WorkHandlerResult;
import com.github.benshi.worker.WorkerHandlerEvent;
import com.github.benshi.worker.WorkerStatus;
import com.github.benshi.worker.cache.LimitsManager;
import com.github.benshi.worker.store.WorkerStore;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class StoreDisruptorHandler extends BaseDisruptorHandler {
    private final WorkerStore workerStore;
    private final RedissonClient redissonClient;
    private final LimitsManager limitsManager;

    @Override
    public void onEvent(WorkerHandlerEvent event) throws Exception {
        try {
            if (event.getCtx() == null || event.getHandler() == null) {
                return;
            }

            WorkContext ctx = event.getCtx();
            WorkerStatus current = ctx.getCurrentStatus();
            // Increment the count
            if (ctx.getHandlerId() != null) {
                limitsManager.incrementCount(ctx.getHandlerId());
            }
            try {
                RLock lock = redissonClient.getLock(ctx.lockKey());
                if (lock.tryLock()) {
                    try {
                        // set worker status to running
                        log.info("Processing job {} for workId {} with handler {}", ctx.getId(), ctx.getWorkId(),
                                ctx.getHandlerId());
                        if (!workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.RUNNING, current,
                                null)) {
                            log.warn("Job {} already running", ctx.getId());
                            return;
                        }
                        current = WorkerStatus.RUNNING;

                        WorkHandlerResult result = event.getHandler().run(new WorkHandlerMessage(ctx.getId(),
                                ctx.getWorkId(), ctx.getPayload()));

                        // Update job as completed in database
                        current = updateJobStatus(ctx, result, current);
                        log.info("Job handlerId ({}), workerId ({}) completed successfully, result: {}",
                                ctx.getHandlerId(),
                                ctx.getWorkId(), result == null ? "NULL" : result.display());
                    } finally {
                        if (lock.isHeldByCurrentThread()) {
                            lock.unlock();
                        }
                    }
                }
            } catch (Exception e) {
                if (ctx.getMaxRetryCount() <= 0) {
                    // Update job as failed permanently in database
                    workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.FAILED_PERMANENT,
                            current,
                            e.getMessage());
                    log.error("Error processing job {} for {} --> {}", ctx.getId(), current,
                            WorkerStatus.FAILED_PERMANENT, e);
                } else {
                    if (ctx.getRetryCount() < ctx.getMaxRetryCount()) {
                        // Update job as retry in database
                        workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.RETRY,
                                current,
                                e.getMessage());
                        log.info("Error processing job {} for {} --> {}", ctx.getId(), current,
                                WorkerStatus.RETRY);
                    } else {
                        // Update job as failed permanently in database
                        workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.FAILED,
                                current,
                                e.getMessage());
                        log.error("Error processing job {} for {} --> {}", ctx.getId(), current,
                                WorkerStatus.FAILED, e);
                    }
                }
            }
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

    private WorkerStatus updateJobStatus(WorkContext ctx, WorkHandlerResult result, WorkerStatus current)
            throws Exception {
        WorkerStatus nextStatus = null;
        if (result == null || result.isSuccess()) {
            nextStatus = WorkerStatus.COMPLETED;
        } else if (result.isRetry()) {
            if (ctx.getMaxRetryCount() <= 0) {
                nextStatus = WorkerStatus.FAILED_PERMANENT;
            } else if (ctx.getRetryCount() < ctx.getMaxRetryCount()) {
                nextStatus = WorkerStatus.RETRY;
            } else {
                nextStatus = WorkerStatus.FAILED;
            }
        } else {
            nextStatus = WorkerStatus.FAILED;
        }

        if (nextStatus == null || nextStatus == current) {
            return current;
        }

        if (workerStore.updateWorkerStatus(ctx.getId(), nextStatus, current, null)) {
            return nextStatus;
        }

        return current;
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, WorkerHandlerEvent event) {
        if (ex instanceof java.lang.InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            log.error("Exception during processing event", ex);
        }

        if (event != null && event.getCtx() != null) {
            try {
                WorkContext ctx = event.getCtx();
                workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.FAILED_PERMANENT,
                        ctx.getCurrentStatus(), ex.getMessage());
            } catch (Exception e) {
                log.error("Error updating job status", e);
            }
        }
    }
}
