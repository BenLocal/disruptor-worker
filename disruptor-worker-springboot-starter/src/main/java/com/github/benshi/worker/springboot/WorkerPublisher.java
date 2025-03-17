package com.github.benshi.worker.springboot;

import com.github.benshi.worker.DisruptorWorker;
import com.github.benshi.worker.WorkHandler;

public class WorkerPublisher {
    private DisruptorWorker worker;

    WorkerPublisher(DisruptorWorker worker) {
        this.worker = worker;
    }

    public void publish(Class<? extends WorkHandler> clazz, String workerId, String payload) {
        if (worker == null) {
            return;
        }
        worker.submit(workerId, clazz.getName(), payload);
    }

    public void publish(Class<? extends WorkHandler> clazz, String workerId, String payload, boolean froce) {
        if (worker == null) {
            return;
        }
        worker.submit(workerId, clazz.getName(), payload, froce);

    }
}
