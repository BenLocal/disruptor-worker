package com.github.benshi.worker;

import lombok.Data;

@Data
public class WorkerHandlerEvent {
    private WorkContext ctx;
    private WorkHandler handler;
}
