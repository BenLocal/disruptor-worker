package com.github.benshi.worker.springboot;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 
 * 
 * @date 2025年6月10日
 * @time 23:00:47
 * @description
 * 
 */
@Data
@Accessors(chain = true)
public class WorkerPublishOptions {
    // 是否强制执行
    private boolean force = false;
    // 是否缓存
    private boolean cache = false;
    // 处理器ID
    private String handlerId;
    // 工作ID
    private String workId;
    // 负载
    private String payload;
    // lock字符串
    // 如果不设置，则使用默认的workId
    private String lockStr = "";
    // 最大重试次数
    private int retryMaxCount = 0;
    // 重试间隔，单位秒
    private int retryIntervalSeconds = 0;
}
