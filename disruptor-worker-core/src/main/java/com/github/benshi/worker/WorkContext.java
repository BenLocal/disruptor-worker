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
    // current retry count
    private int retryCount;
    // 强制运行
    private boolean force;
    // 最大重试次数
    private int maxRetryCount;
    // retry interval seconds
    private int retryIntervalSeconds;
    // custom lock string
    // 如果不设置，则使用默认的 lockKey
    private String lockStr;

    private final static String BASE_WORKER_KEY = "worker-lock-";

    public String lockKey() {
        if (this.lockStr != null && !this.lockStr.isEmpty()) {
            return String.format("%s-%s-%s", BASE_WORKER_KEY, this.handlerId, this.lockStr);
        }

        return String.format("%s-%s-%s", BASE_WORKER_KEY, this.handlerId, this.workId);
    }

    public String bidDisplay() {
        return String.format("%s(%s)", this.handlerId, this.workId);
    }
}
