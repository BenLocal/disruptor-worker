package com.github.benshi.worker;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class WorkHandlerMessage {
    // unique id for the message
    private long id;
    private String workId;
    private String payload;
}
