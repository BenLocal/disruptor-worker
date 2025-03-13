package com.github.benshi.worker;

public interface WorkHandler {
    WorkHandlerResult run(WorkHandlerMessage msg);
}
