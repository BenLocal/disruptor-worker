package com.github.benshi.worker.springboot.vo;

import java.util.List;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ListResponse {

    private List<ListItem> items;
    private long total;

    @Data
    @Accessors(chain = true)
    public static class ListItem {
        private long id;
        private String workId;
        private String handlerId;
        private String payload;
        private String status;
        private int retryCount;
        private int maxRetryCount;
    }
}
