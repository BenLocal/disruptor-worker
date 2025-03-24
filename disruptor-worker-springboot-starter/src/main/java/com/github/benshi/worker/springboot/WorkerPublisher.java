package com.github.benshi.worker.springboot;

import com.github.benshi.worker.DisruptorWorker;
import com.github.benshi.worker.WorkHandler;

public class WorkerPublisher {
    private DisruptorWorker worker;

    WorkerPublisher(DisruptorWorker worker) {
        this.worker = worker;
    }

    public void publish(Class<? extends WorkHandler> clazz, String workerId, String payload) {
        publish(clazz.getName(), workerId, payload, false);
    }

    public void publish(Class<? extends WorkHandler> clazz, String workerId, String payload, boolean froce) {
        publish(clazz.getName(), workerId, payload, froce);
    }

    public void publish(String handlerId, String workerId, String payload, boolean froce) {
        if (worker == null) {
            return;
        }
        worker.submit(workerId, handlerId, payload, froce);
    }
}
