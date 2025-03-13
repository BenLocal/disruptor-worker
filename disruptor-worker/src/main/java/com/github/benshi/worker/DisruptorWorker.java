package com.github.benshi.worker;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.github.benshi.worker.store.MysqlWorkerStore;
import com.github.benshi.worker.store.WorkerStore;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.WorkProcessor;
import com.lmax.disruptor.dsl.Disruptor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DisruptorWorker {
    // 1 second
    private static final int DEFAULT_POLL_INTERVAL_MS = 1000;

    private final ExecutorService executor;
    private final Sequence workSequence;
    private final ScheduledExecutorService scheduler;
    private final Map<Integer, WorkProcessor<?>> workProcessors;
    private final Disruptor<WorkerHandlerEvent> disruptor;
    private final RingBuffer<WorkerHandlerEvent> ringBuffer;
    private final WorkerStore workerStore;

    private final Map<String, WorkHandler> jobHandlers = new HashMap<>();
    private final Map<String, Integer> jobLimits = new ConcurrentHashMap<>();

    public DisruptorWorker(DataSource dataSource, int bufferSize, int coreSize, int maxSize) {
        this.executor = new ThreadPoolExecutor(
                coreSize,
                maxSize,
                120L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new DisruptorWorkerThreadFactory("d-worker"));
        this.workSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        this.disruptor = new Disruptor<>(
                WorkerHandlerEvent::new,
                bufferSize,
                new DisruptorWorkerThreadFactory("d-main"));
        this.workProcessors = new HashMap<>();
        this.workerStore = new MysqlWorkerStore(dataSource);

        // Initialize scheduler for periodic database polling
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DisruptorWorkerThreadFactory("job-poller"));

        DisruptorHandler handler = new DisruptorHandler(workerStore);
        this.disruptor.setDefaultExceptionHandler(handler);
        this.ringBuffer = disruptor.getRingBuffer();
        for (int i = 0; i < coreSize; i++) {
            WorkProcessor<WorkerHandlerEvent> wp = new WorkProcessor<>(
                    ringBuffer,
                    ringBuffer.newBarrier(),
                    handler,
                    handler,
                    workSequence);
            this.executor.execute(wp);
            this.workProcessors.put(i, wp);
        }
    }

    public void start() {
        this.disruptor.start();

        // Start polling the database
        this.scheduler.scheduleWithFixedDelay(
                this::pollWorkersFromDatabase,
                1000, // Initial delay of 1 second
                DEFAULT_POLL_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        log.info("DisruptorWorker started, polling database every {} ms", DEFAULT_POLL_INTERVAL_MS);
    }

    public void shutdown() {
        log.info("Shutting down DisruptorWorker...");

        // Stop the scheduler first
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }

        // Then shutdown the disruptor
        if (disruptor != null) {
            disruptor.shutdown();
        }

        // Finally shutdown the executor
        if (executor != null && !executor.isShutdown()) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }

        log.info("DisruptorWorker shutdown complete");
    }

    public void register(String handlerId, WorkHandler handler, int limit) throws WorkerException {
        if (jobHandlers.containsKey(handlerId)) {
            throw new WorkerException("handlerId already registered");
        }

        jobHandlers.put(handlerId, handler);
        jobLimits.put(handlerId, limit);
    }

    public boolean submit(WorkContext ctx) {
        if (ctx == null || ctx.getHandlerId() == null) {
            return false;
        }

        try {
            ctx.setCurrentStatus(WorkerStatus.PENDING);
            if (!workerStore.saveWorker(ctx)) {
                log.error("Error saving job to database: {}", ctx.getId());
                return false;
            }
        } catch (Exception e) {
            if (e instanceof SQLIntegrityConstraintViolationException) {
                log.warn("Job {} already exists in database", ctx.getId());
            } else {
                log.error("Error saving job to database: {}", ctx.getId(), e);
            }
            return false;
        }

        return true;

    }

    // Method to poll workers from the database and submit to ring buffer
    private void pollWorkersFromDatabase() {
        try {
            // Get jobs with PENDING status
            // Fetch up to 100 jobs at a time
            List<WorkContext> pendingJobs = this.workerStore.getPendingWorkers(100);
            if (pendingJobs.isEmpty()) {
                return;
            }

            for (WorkContext ctx : pendingJobs) {
                WorkHandler handler = jobHandlers.get(ctx.getHandlerId());
                if (handler == null) {
                    log.warn("No handler found for job {}, handlerId: {}", ctx.getId(), ctx.getHandlerId());
                    continue;
                }

                // Check handler limit
                Integer limit = jobLimits.get(ctx.getHandlerId());
                if (limit != null && limit > 0) {
                    // TODO check local cache running count for this handler
                }

                // Check ring buffer capacity
                if (ringBuffer.remainingCapacity() <= 1) {
                    log.warn("Ring buffer capacity too low: {}", ringBuffer.remainingCapacity());
                    break; // Stop processing more jobs
                }

                // Add to ring buffer for processing
                long sequence = ringBuffer.next();
                try {
                    WorkerHandlerEvent event = ringBuffer.get(sequence);
                    event.setCtx(ctx);
                    event.setHandler(handler);
                    log.debug("Job {} added to ring buffer for processing", ctx.getId());
                } finally {
                    ringBuffer.publish(sequence);
                }
            }
        } catch (Exception e) {
            log.error("Error polling jobs from database", e);
        }
    }
}
