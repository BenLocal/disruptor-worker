package com.github.benshi.worker.store.dao;

import java.util.Date;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class WorkerJob {
    private Long id;
    private String workId;
    private String handlerId;
    private String payload;
    private String status;
    private Integer priority;
    private Integer retryCount;
    private String message;
    private Date createdAt;
    private Date updatedAt;
}
