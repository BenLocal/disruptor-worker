package com.github.benshi.worker;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Calendar;
import java.util.Date;
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

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import com.github.benshi.worker.cache.LimitsManager;
import com.github.benshi.worker.cache.LocalLimitsManager;
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
    private static final int DEFAULT_POLL_INTERVAL_MS = 3000;

    private final ExecutorService executor;
    private final Sequence workSequence;
    private final ScheduledExecutorService scheduler;
    private final Map<Integer, WorkProcessor<?>> workProcessors;
    private final Disruptor<WorkerHandlerEvent> disruptor;
    private final RingBuffer<WorkerHandlerEvent> ringBuffer;
    private final WorkerStore workerStore;
    private final RedissonClient redissonClient;
    private final LimitsManager limitsManager;
    private final int bufferSize;
    private final int stayDays;

    private final Map<String, WorkHandler> jobHandlers = new HashMap<>();
    private final Map<String, Integer> jobLimits = new ConcurrentHashMap<>();

    // Get available CPU cores
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    public DisruptorWorker(RedissonClient redissonClient,
            DataSource dataSource) {
        // Limit to 4 threads
        this(redissonClient, dataSource, 1024, 7);
    }

    public DisruptorWorker(RedissonClient redissonClient,
            DataSource dataSource, int stayDays) {
        // Limit to 4 threads
        this(redissonClient, dataSource, 1024, stayDays);
    }

    public DisruptorWorker(RedissonClient redissonClient,
            DataSource dataSource, int bufferSize, int stayDays) {
        // Limit to 4 threads
        this(redissonClient, dataSource,
                bufferSize,
                Math.max(AVAILABLE_PROCESSORS, 4),
                Math.max(AVAILABLE_PROCESSORS * 2, 8),
                stayDays);
    }

    public DisruptorWorker(RedissonClient redissonClient,
            DataSource dataSource,
            int bufferSize,
            int coreSize,
            int maxSize,
            int stayDays) {
        this.bufferSize = bufferSize;
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
        this.redissonClient = redissonClient;
        this.limitsManager = new LocalLimitsManager();
        this.stayDays = stayDays;

        // Initialize scheduler for periodic database polling
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DisruptorWorkerThreadFactory("job-poller"));

        DisruptorHandler handler = new DisruptorHandler(this.workerStore,
                this.redissonClient, this.limitsManager);
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
        // Initial delay of 1 second
        this.scheduler.scheduleWithFixedDelay(
                this::pollPendingWorkersFromDatabase,
                1000,
                DEFAULT_POLL_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        log.info("DisruptorWorker started, polling database every {} ms", DEFAULT_POLL_INTERVAL_MS);

        // Start polling the datebase for running jobs
        // Initial delay of 1 second
        // Poll every 5 minutes
        this.scheduler.scheduleWithFixedDelay(
                this::pollRunningWorkersFromDatabase,
                1000,
                1000 * 60 * 5,
                TimeUnit.MILLISECONDS);

        // Clear jobs with all statuses except RUNNING
        // Initial delay of 1 second
        // Poll every 1 day
        this.scheduler.scheduleWithFixedDelay(
                this::clearJobs,
                1000,
                1000 * 60 * 60 * 24,
                TimeUnit.MILLISECONDS);
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

    public boolean submit(String workId, String handerId, String payload) {
        return submit(workId, handerId, payload, false);
    }

    public boolean submit(String workId, String handerId, String payload, boolean force) {
        WorkContext ctx = new WorkContext()
                .setWorkId(workId)
                .setHandlerId(handerId)
                .setPayload(payload);

        return submit(ctx, force);
    }

    private boolean submit(WorkContext ctx, boolean force) {
        if (ctx == null || ctx.getHandlerId() == null) {
            return false;
        }

        try {
            ctx.setCurrentStatus(WorkerStatus.PENDING);
            if (force) {
                WorkContext db = workerStore.getWorkerByWorkId(ctx.getWorkId(), ctx.getHandlerId());
                if (db != null && db.getCurrentStatus() != WorkerStatus.RUNNING) {
                    RLock lock = redissonClient.getLock(ctx.lockKey());
                    if (lock.tryLock(10, 10, TimeUnit.SECONDS)) {
                        try {
                            // Update job status to PENDING
                            log.info("Job {}<<<{}>>> is marked as RUNNING but has no active lock, resetting to PENDING",
                                    db.getId(), db.bidDisplay());
                            workerStore.updateWorkerStatus(db.getId(),
                                    WorkerStatus.PENDING,
                                    db.getCurrentStatus(),
                                    "Reset due to no active lock");
                            log.info("Job {}<<<{}>>> reset to PENDING", db.getId(), db.bidDisplay());
                        } finally {
                            lock.unlock();
                        }
                        return true;
                    }
                }
            }

            if (!workerStore.saveWorker(ctx)) {
                log.error("Error saving job to database: {}", ctx.bidDisplay());
                return false;
            }
        } catch (

        Exception e) {
            if (e instanceof SQLIntegrityConstraintViolationException) {

                log.warn("Job {} already exists in database", ctx.bidDisplay());
            } else {
                log.error("Error saving job to database: {}", ctx.bidDisplay(), e);
            }
            return false;
        }

        return true;

    }

    private void pollRunningWorkersFromDatabase() {
        try {
            List<WorkContext> runningJobs = this.workerStore.getWorkersByStatus(WorkerStatus.RUNNING,
                    this.bufferSize * 2);
            if (runningJobs.isEmpty()) {
                return;
            }
            for (WorkContext ctx : runningJobs) {
                RLock lock = redissonClient.getLock(ctx.lockKey());
                if (lock.isLocked() && !lock.isHeldByCurrentThread()) {
                    // Job is still running, skip
                    continue;
                }

                // update job status to PANDING
                log.info("Job {} is marked as RUNNING but has no active lock, resetting to PENDING", ctx.getId());
                ctx.setCurrentStatus(WorkerStatus.PENDING);
                workerStore.updateWorkerStatus(ctx.getId(), WorkerStatus.PENDING,
                        ctx.getCurrentStatus(),
                        "Reset due to no active lock");
                log.info("Job {} reset to PENDING", ctx.getId());

            }
        } catch (Exception e) {
            log.error("Error polling running jobs from database", e);
        }
    }

    // Method to poll workers from the database and submit to ring buffer
    private void pollPendingWorkersFromDatabase() {
        try {
            // Get jobs with PENDING status
            // Fetch up to 100 jobs at a time
            List<WorkContext> pendingJobs = this.workerStore.getWorkersByStatus(WorkerStatus.PENDING, 100);
            if (pendingJobs.isEmpty()) {
                return;
            }

            for (WorkContext ctx : pendingJobs) {
                WorkHandler handler = jobHandlers.get(ctx.getHandlerId());
                if (handler == null) {
                    log.debug("No handler found for job {}, handlerId: {}", ctx.getId(), ctx.getHandlerId());
                    continue;
                }

                // Check handler limit
                Integer limit = jobLimits.get(ctx.getHandlerId());
                if (limit != null && limit > 0) {
                    long currentCount = limitsManager.getCount(ctx.getHandlerId());
                    if (currentCount >= limit) {
                        log.debug("Job limit reached for handler {}: {} >= {}", ctx.getHandlerId(), currentCount,
                                limit);
                        continue;
                    }
                }

                // Check ring buffer capacity
                if (ringBuffer.remainingCapacity() <= 1) {
                    log.warn("Ring buffer capacity too low: {}", ringBuffer.remainingCapacity());
                    // Stop processing more jobs
                    break;
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

    public void clearJobs() {
        try {
            // Calculate the date N days ago
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, -stayDays);
            Date cutoffDate = calendar.getTime();

            log.info("Clearing jobs older than {} days (before {})", stayDays, cutoffDate);

            // Delete all jobs except RUNNING ones that are older than the cutoff date
            int deletedCount = workerStore.deleteJobsOlderThan(cutoffDate, WorkerStatus.RUNNING);

            log.info("Deleted {} old jobs from the database", deletedCount);
        } catch (Exception e) {
            log.error("Error clearing jobs from database", e);
        }
    }
}
