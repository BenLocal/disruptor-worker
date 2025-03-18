package com.github.benshi.worker.springboot.vo;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class PublishResponse {
    private boolean status;
    private String message;
}
