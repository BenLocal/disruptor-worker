package com.github.benshi.worker;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class WorkHandlerResult {
    private final boolean success;

    public static WorkHandlerResult success() {
        return new WorkHandlerResult(true);
    }
}
