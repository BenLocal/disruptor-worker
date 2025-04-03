package com.github.benshi.worker.handler;

import com.github.benshi.worker.WorkerHandlerEvent;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WorkHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseDisruptorHandler
        implements WorkHandler<WorkerHandlerEvent>, ExceptionHandler<WorkerHandlerEvent> {

    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("Exception during start", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("Exception during shutdown", ex);
    }
}
