package com.github.benshi.worker.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "disruptor.worker")
public class WorkerProperties {
    private int bufferSize = 1024;

    private int stayDays = 7;

    private String storeDirver = "mysql-jdbc";
}
