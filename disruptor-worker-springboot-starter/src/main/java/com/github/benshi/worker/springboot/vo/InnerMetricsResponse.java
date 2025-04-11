package com.github.benshi.worker.springboot.vo;

import java.util.Map;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class InnerMetricsResponse {
    private Map<String, Object> ringBuffer;
}
