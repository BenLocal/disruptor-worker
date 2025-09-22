package com.github.benshi.worker.example;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.github.benshi.worker.PublishResult;
import com.github.benshi.worker.WorkHandlerMessage;
import com.github.benshi.worker.WorkHandlerResult;
import com.github.benshi.worker.WorkerHandler;
import com.github.benshi.worker.springboot.Worker;
import com.github.benshi.worker.springboot.WorkerPublisher;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
@Worker(cache = true)
public class OtherCacheTaskWorker implements WorkerHandler {
    private final WorkerPublisher workerPublisher;

    @Override
    public WorkHandlerResult run(WorkHandlerMessage msg) throws Exception {
        try {
            Thread.sleep(1000); // simulate work
            return WorkHandlerResult.success();
        } catch (Exception e) {
            e.printStackTrace();
            return WorkHandlerResult.failure();
        }
    }

    private final AtomicLong count = new AtomicLong(0);

    @Scheduled(fixedRate = 1000)
    public void job1() {
        long a = count.incrementAndGet();
        String key = "other cache job" + a;
        PublishResult res = workerPublisher.publish(
                OtherCacheTaskWorker.class, key, String.valueOf(a),
                true);
        if (!res.isSuccess()) {
            // Handle failure
            System.out.println("Failed to publish job: " + key);
        }
    }
}
