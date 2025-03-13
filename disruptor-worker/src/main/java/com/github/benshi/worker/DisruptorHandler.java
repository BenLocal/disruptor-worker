package com.github.benshi.worker;

import com.github.benshi.worker.store.WorkerStore;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WorkHandler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DisruptorHandler implements WorkHandler<WorkerHandlerEvent>, ExceptionHandler<WorkerHandlerEvent> {
    private final WorkerStore workerStore;

    @Override
    public void onEvent(WorkerHandlerEvent event) throws Exception {
        if (event.getCtx() == null || event.getHandler() == null) {
            return;
        }
        WorkContext ctx = event.getCtx();

        try {
            // set worker status to running
            log.info("Processing job {} with handler {}", ctx.getId(), ctx.getHandlerId());
            if (!workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.RUNNING, ctx.getCurrentStatus(), null)) {
                log.warn("Job {} already running", ctx.getId());
                return;
            }

            event.getHandler().run(new WorkHandlerMessage(ctx.getId(), ctx.getPayload()));

            // Update job as completed in database
            workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.COMPLETED, ctx.getCurrentStatus(), null);
            log.info("Job {} completed successfully", ctx.getId());
        } catch (Exception e) {
            log.error("Error processing job {}", ctx.getId(), e);
            workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.FAILED, ctx.getCurrentStatus(), e.getMessage());
        }
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, WorkerHandlerEvent event) {
        log.error("Exception during processing event", ex);
        if (event != null && event.getCtx() != null) {

            try {
                WorkContext ctx = event.getCtx();
                workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.FAILED_PERMANENT,
                        ctx.getCurrentStatus(), ex.getMessage());
                // int maxRetries = worker.getMaxRetriesForHandler(ctx.getHandlerId());
                // if (ctx.getRetryCount() < maxRetries) {
                // workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.RETRY,
                // ex.getMessage());
                // } else {
                // workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.FAILED_PERMANENT,
                // ex.getMessage());
                // }
            } catch (Exception e) {
                log.error("Error updating job status", e);
            }
        }
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        System.out.println("handleOnStartException");
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        System.out.println("handleOnShutdownException");
    }

}
