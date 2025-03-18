package com.github.benshi.worker.springboot.vo;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ListRequest {
    private int page;
    private int size;
    private String filter;
}
