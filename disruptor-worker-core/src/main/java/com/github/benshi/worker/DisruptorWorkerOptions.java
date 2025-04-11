package com.github.benshi.worker;

import java.util.Properties;

import javax.sql.DataSource;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@Builder
public class DisruptorWorkerOptions {
    private String storeName;
    private DataSource dataSource;
    private int stayDays;
    private int bufferSize;
    private int coreSize;
    private int maxSize;
    private Properties properties;
    private boolean dynamicWorker;

    // Get available CPU cores
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    public int getCoreSize() {
        return coreSize > 0 ? coreSize : Math.max(AVAILABLE_PROCESSORS, 4);
    }

    public int getMaxSize() {
        return maxSize > 0 ? maxSize : Math.max(AVAILABLE_PROCESSORS * 2, 8);
    }

    public int getBufferSize() {
        return bufferSize > 0 ? bufferSize : 1024;
    }

    public int getStayDays() {
        return stayDays > 0 ? stayDays : 7;
    }
}
