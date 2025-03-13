package com.github.benshi.worker;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class WorkHandlerMessage {
    private String workId;
    private String payload;

    public WorkHandlerMessage(String workId, String payload) {
        this.workId = workId;
        this.payload = payload;
    }
}
