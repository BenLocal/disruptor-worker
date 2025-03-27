package com.github.benshi.worker.springboot;

import java.util.List;
import java.util.stream.Collectors;

import javax.websocket.server.PathParam;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import com.github.benshi.worker.DisruptorWorker;
import com.github.benshi.worker.WorkContext;
import com.github.benshi.worker.springboot.vo.ListRequest;
import com.github.benshi.worker.springboot.vo.ListResponse;
import com.github.benshi.worker.springboot.vo.PublishResponse;
import com.github.benshi.worker.store.WorkerStore;

import lombok.RequiredArgsConstructor;

@Controller
@RequestMapping("/workers")
@RequiredArgsConstructor
public class ApiController {
    public final WorkerPublisher workerPublisher;
    public final DisruptorWorker worker;

    @PostMapping("/publish/{id}")
    public ResponseEntity<PublishResponse> publish(@PathParam("id") long id) {
        try {
            WorkerStore workerStore = worker.getWorkerStore();
            WorkContext ctx = workerStore.getWorkerById(id);
            if (ctx == null) {
                return ResponseEntity.ok(new PublishResponse()
                        .setStatus(false)
                        .setMessage("worker not found"));
            }

            workerPublisher.publish(ctx.getHandlerId(), ctx.getWorkId(), ctx.getPayload(), true);
            return ResponseEntity.ok(new PublishResponse()
                    .setStatus(true));
        } catch (Exception e) {
            return ResponseEntity.ok(new PublishResponse()
                    .setStatus(false)
                    .setMessage(e.getMessage()));
        }
    }

    @PostMapping("/list")
    public ResponseEntity<ListResponse> list(@RequestBody ListRequest request) {
        try {

            if (request == null) {
                return ResponseEntity.ok(new ListResponse());
            }

            long count = worker.getWorkerStore().count(request.getFilter());
            if (count == 0) {
                return ResponseEntity.ok(new ListResponse());
            }

            int offset = (request.getPage() - 1) * request.getSize();
            int size = request.getSize();
            List<WorkContext> list = worker.getWorkerStore().list(offset, size, request.getFilter());
            if (list == null) {
                return ResponseEntity.ok(new ListResponse());
            }

            List<ListResponse.ListItem> items = list.stream()
                    .map((WorkContext ctx) -> new ListResponse.ListItem()
                            .setId(ctx.getId())
                            .setHandlerId(ctx.getHandlerId())
                            .setPayload(ctx.getPayload())
                            .setStatus(ctx.getCurrentStatus().name())
                            .setRetryCount(ctx.getRetryCount())
                            .setWorkId(ctx.getWorkId())
                            .setMaxRetryCount(ctx.getMaxRetryCount()))
                    .collect(Collectors.toList());
            return ResponseEntity.ok(new ListResponse()
                    .setItems(items)
                    .setTotal(count));
        } catch (Exception e) {
            return ResponseEntity.ok(new ListResponse());
        }

    }
}
