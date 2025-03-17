package com.github.benshi.worker.cache;

public interface LimitsManager {

    /**
     * Increment the count for the given handler ID
     * 
     * @param handlerId the handler ID
     * @return the new count
     */
    long incrementCount(String handlerId);

    /**
     * Decrement the count for the given handler ID
     * 
     * @param handlerId the handler ID
     * @return the new count
     */
    long decrementCount(String handlerId);

    /**
     * Get the current count for the given handler ID
     * 
     * @param handlerId the handler ID
     * @return the current count
     */
    long getCount(String handlerId);
}
