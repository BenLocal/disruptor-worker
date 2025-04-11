package com.github.benshi.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.redisson.api.RedissonClient;

import com.github.benshi.worker.handler.BaseDisruptorHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.WorkProcessor;
import com.lmax.disruptor.dsl.Disruptor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseDisruptorWorker {
    protected final ExecutorService executor;
    protected final Sequence workSequence;
    protected final ScheduledExecutorService scheduler;
    protected final Map<Integer, WorkProcessor<?>> workProcessors;
    protected final Disruptor<WorkerHandlerEvent> disruptor;
    protected final RingBuffer<WorkerHandlerEvent> ringBuffer;
    protected final RedissonClient redissonClient;
    protected final int bufferSize;
    protected final int stayDays;
    protected final BaseDisruptorHandler handler;

    protected final Map<String, WorkerHandler> jobHandlers = new HashMap<>();
    protected final Map<String, Integer> jobLimits = new ConcurrentHashMap<>();

    protected final int maxAdditionalWorkers;
    protected final Object workerScalingLock = new Object();
    protected final AtomicInteger currentWorkerCount = new AtomicInteger(0);
    protected final float highWaterMarkPercentage = 70.0f;
    protected final float lowWaterMarkPercentage = 30.0f;

    protected BaseDisruptorWorker(RedissonClient redissonClient,
            BaseDisruptorHandler handler,
            DisruptorWorkerOptions options) {
        this.bufferSize = options.getBufferSize();
        int coreSize = options.getCoreSize();
        int maxSize = options.getMaxSize();
        this.handler = handler;
        this.stayDays = options.getStayDays();
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
        this.redissonClient = redissonClient;

        // Initialize scheduler for periodic database polling
        this.scheduler = Executors.newScheduledThreadPool(3,
                new DisruptorWorkerThreadFactory("job-poller"));

        this.disruptor.setDefaultExceptionHandler(handler);
        this.ringBuffer = disruptor.getRingBuffer();
        for (int i = 0; i < options.getCoreSize(); i++) {
            WorkProcessor<WorkerHandlerEvent> wp = new WorkProcessor<>(
                    ringBuffer,
                    ringBuffer.newBarrier(),
                    handler,
                    handler,
                    workSequence);
            this.executor.execute(wp);
            this.workProcessors.put(i, wp);
        }

        if (options.isDynamicWorker()) {
            this.maxAdditionalWorkers = options.getMaxSize() - options.getCoreSize();
            this.scheduler.scheduleAtFixedRate(
                    this::monitorAndAdjustWorkerCount,
                    5, 5, TimeUnit.SECONDS);
        } else {
            this.maxAdditionalWorkers = 0;
        }

        log.info("DisruptorWorker initialized with buffer size: {}, core size: {}, max size: {}, dynamic: {}",
                bufferSize, coreSize, maxSize, options.isDynamicWorker());
    }

    public void start() {
        disruptor.start();
    }

    public void shutdown() {
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
    }

    public void register(String handlerId, WorkerHandler handler, int limit) throws WorkerException {
        if (jobHandlers.containsKey(handlerId)) {
            throw new WorkerException("handlerId already registered");
        }

        jobHandlers.put(handlerId, handler);
        jobLimits.put(handlerId, limit);
    }

    public int registerCount() {
        return jobHandlers.size();
    }

    public Map<String, Object> getRingBufferMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("bufferSize", ringBuffer.getBufferSize());
        metrics.put("cursor", ringBuffer.getCursor());
        metrics.put("remainingCapacity", ringBuffer.remainingCapacity());
        // Calculate buffer utilization percentage
        long used = ringBuffer.getBufferSize() - ringBuffer.remainingCapacity();
        double utilizationPercentage = (double) used / ringBuffer.getBufferSize() * 100;
        metrics.put("utilizationPercentage", utilizationPercentage);
        return metrics;
    }

    private void monitorAndAdjustWorkerCount() {
        try {
            long bufferSize = ringBuffer.getBufferSize();
            long remainingCapacity = ringBuffer.remainingCapacity();
            long used = bufferSize - remainingCapacity;
            double utilizationPercentage = (double) used / bufferSize * 100;

            int currentCount = currentWorkerCount.get();
            int coreSize = this.workProcessors.size();
            int maxSize = coreSize + maxAdditionalWorkers;

            synchronized (workerScalingLock) {
                // Scale up if utilization is high and we haven't reached max workers
                if (utilizationPercentage > highWaterMarkPercentage && currentCount < maxSize) {
                    int numToAdd = Math.min(2, maxSize - currentCount); // Add up to 2 workers at a time
                    for (int i = 0; i < numToAdd; i++) {
                        int newIndex = currentCount + i;
                        addWorkerProcessor(newIndex, this.handler);
                    }
                    log.info("Scaled up worker count to {} due to high buffer utilization: {}%",
                            currentWorkerCount.get(), String.format("%.2f", utilizationPercentage));
                }

                // Scale down if utilization is low and we're above core size
                else if (utilizationPercentage < lowWaterMarkPercentage && currentCount > coreSize) {
                    int numToRemove = Math.min(1, currentCount - coreSize); // Remove 1 worker at a time
                    for (int i = 0; i < numToRemove; i++) {
                        int indexToRemove = currentCount - 1 - i;
                        removeWorkerProcessor(indexToRemove);
                    }
                    log.info("Scaled down worker count to {} due to low buffer utilization: {}%",
                            currentWorkerCount.get(), String.format("%.2f", utilizationPercentage));
                }
            }
        } catch (Exception e) {
            log.error("Error monitoring or adjusting worker count", e);
        }
    }

    private void addWorkerProcessor(int index, BaseDisruptorHandler handler) {
        WorkProcessor<WorkerHandlerEvent> wp = new WorkProcessor<>(
                ringBuffer,
                ringBuffer.newBarrier(),
                handler,
                handler,
                workSequence);
        this.executor.execute(wp);
        this.workProcessors.put(index, wp);
        currentWorkerCount.incrementAndGet();
    }

    private void removeWorkerProcessor(int index) {
        WorkProcessor<?> wp = workProcessors.remove(index);
        if (wp != null) {
            wp.halt();
            currentWorkerCount.decrementAndGet();
        }
    }

    public abstract boolean submit(WorkContext ctx);
}
