package com.github.benshi.worker;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class WorkHandlerResult {
    private final boolean success;
    private final Exception exception;
    private final boolean retry;

    public static WorkHandlerResult success() {
        return new WorkHandlerResult(true, null, false);
    }

    public static WorkHandlerResult failure() {
        return new WorkHandlerResult(false, null, false);
    }

    public static WorkHandlerResult failure(Exception exception) {
        return new WorkHandlerResult(false, exception, false);
    }

    public static WorkHandlerResult retry() {
        return new WorkHandlerResult(false, null, true);
    }

    public String display() {
        return String.format("WorkHandlerResult{success=%s, exception=%s, retry=%s}",
                success,
                exception != null ? exception.getMessage() : "null",
                retry);
    }
}
