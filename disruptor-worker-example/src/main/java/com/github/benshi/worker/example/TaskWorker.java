package com.github.benshi.worker.example;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.github.benshi.worker.WorkHandlerMessage;
import com.github.benshi.worker.WorkHandlerResult;
import com.github.benshi.worker.WorkerHandler;
import com.github.benshi.worker.springboot.Worker;
import com.github.benshi.worker.springboot.WorkerPublisher;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
@Worker
public class TaskWorker implements WorkerHandler {
    private final WorkerPublisher workerPublisher;

    @Override
    public WorkHandlerResult run(WorkHandlerMessage msg) throws Exception {
        try {
            System.out.println("TaskWorker: " + msg);
            Thread.sleep(20000); // simulate work
            System.out.println("TaskWorker: " + msg + " done");
            return WorkHandlerResult.success();
        } catch (Exception e) {
            e.printStackTrace();
            return WorkHandlerResult.failure();
        }
    }

    private final AtomicLong count = new AtomicLong(0);

    @Scheduled(fixedRate = 19000)
    public void job1() {
        long a = count.incrementAndGet();
        workerPublisher.publish(TaskWorker.class, "job1" + a, String.valueOf(a),
                true);
    }
}
