package com.github.benshi.worker.springboot.vo;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class MetricsResponse {
    private InnerMetricsResponse db;
    private InnerMetricsResponse cache;
}
