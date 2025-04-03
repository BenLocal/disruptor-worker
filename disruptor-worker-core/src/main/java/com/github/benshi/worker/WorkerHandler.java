package com.github.benshi.worker;

public interface WorkerHandler {
    WorkHandlerResult run(WorkHandlerMessage msg) throws Exception;
}
