package com.github.benshi.worker;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class WorkContext {
    // worker id
    private String id;
    // handler id
    private String handlerId;
    // payload
    private String payload;
    // current status
    private WorkerStatus currentStatus;
    // next status
    private int currentRetryCount;
}
