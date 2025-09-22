package com.github.benshi.worker;

import lombok.Data;

@Data
public class WorkerHandlerEvent {
    private WorkContext ctx;
    private WorkerHandler handler;
    private Integer limit;
}
