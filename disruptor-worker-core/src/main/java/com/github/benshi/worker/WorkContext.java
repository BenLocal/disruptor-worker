package com.github.benshi.worker;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class WorkContext {
    // id
    private long id;
    // worker id
    private String workId;
    // handler id
    private String handlerId;
    // payload
    private String payload;
    // current status
    private WorkerStatus currentStatus;
    // next status
    private int currentRetryCount;
    // 强制运行
    private boolean force;

    private final static String BASE_WORKER_KEY = "worker-lock-";

    public String lockKey() {
        return String.format("%s-%s-%s", BASE_WORKER_KEY, this.handlerId, this.workId);
    }

    public String bidDisplay() {
        return String.format("%s(%s)", this.handlerId, this.workId);
    }
}
