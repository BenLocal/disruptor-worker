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

    protected final Map<String, WorkerHandler> jobHandlers = new HashMap<>();
    protected final Map<String, Integer> jobLimits = new ConcurrentHashMap<>();

    protected BaseDisruptorWorker(RedissonClient redissonClient,
            BaseDisruptorHandler handler,
            DisruptorWorkerOptions options) {
        this.bufferSize = options.getBufferSize();
        int coreSize = options.getCoreSize();
        int maxSize = options.getMaxSize();
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

    public abstract boolean submit(WorkContext ctx);
}
