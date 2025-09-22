package com.github.benshi.worker;

/**
 * 
 * 
 * @date 2025年9月22日
 * @time 22:10:58
 * @description
 * 
 */
public enum PublishResult {
    SUCCESS,
    ARGS_ERROR,
    HANDLER_NULL,
    CAPACITY_FULL,
    LIMIT_REACHED,
    LOCKED,
    EXCEPTION,
    STORE_ERROR,
    ;

    public boolean isSuccess() {
        return this == SUCCESS;
    }
}
