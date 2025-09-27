package cws.k8s.scheduler.rest;

import cws.k8s.scheduler.local.AdminClusterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Thin REST adapter delegating all logic to AdminClusterService.
 *
 * Endpoints (unchanged):
 *
 *  TASKS:
 *    POST   /v1/admin/scheduler/{execution}/task
 *    DELETE /v1/admin/scheduler/{execution}/task/{id}
 *    GET    /v1/admin/scheduler/{execution}/tasks/state?ids=1,2
 *    GET    /v1/admin/scheduler/{execution}/tasks/summary?ids=...
 *
 *  NODES:
 *    POST   /v1/admin/cluster/node
 *    DELETE /v1/admin/cluster/node/{name}
 *    PATCH  /v1/admin/cluster/node/{name}/labels
 *
 *  PODS:
 *    POST   /v1/admin/cluster/pod
 *    DELETE /v1/admin/cluster/pod/{namespace}/{name}
 *
 *  CLUSTER INFO:
 *    GET    /v1/admin/cluster/pods/count
 *    GET    /v1/admin/cluster/pods
 *    GET    /v1/admin/cluster/nodes/count
 *    GET    /v1/admin/cluster/nodes
 *    GET    /v1/admin/cluster/snapshot (optional params executions, taskIds)
 *
 *  TRACKING:
 *    POST   /v1/admin/scheduler/{execution}/track
 *    GET    /v1/admin/scheduler/tracked
 */
@RestController
@RequestMapping("/v1/admin")
@RequiredArgsConstructor
@Slf4j
public class RestClusterInterface {

    private final AdminClusterService service;

    /* =============================================================
       DTOs
       ============================================================= */
    public record CreateTaskRequest(
            int id,
            String task,
            String name,
            String runName,
            String workDir,
            Float cpus,          // (Info – aktuell nicht im TaskConfig verwendbar)
            Long memoryBytes
    ) {}

    public record CreateNodeRequest(
            String name,
            String cpu,
            String memory,
            Map<String,String> labels
    ) {}

    public record PatchNodeLabelsRequest(
            Map<String,String> labels
    ) {}

    public record CreatePodRequest(
            String name,
            String namespace,
            String nodeName,
            String image,
            List<String> command,
            String ip,
            Map<String,String> nodeSelector,
            Map<String,String> labels,
            String cpuRequest,
            String memoryRequest
    ) {}

    /* =============================================================
       TASK ENDPOINTS
       ============================================================= */

    @DeleteMapping("/scheduler/{execution}/task/{id}")
    public ResponseEntity<?> removeTask(@PathVariable String execution,
                                        @PathVariable int id) {
        boolean removed = service.removeTask(execution, id);
        return removed
                ? ResponseEntity.ok(Map.of("removed", true, "taskId", id))
                : ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("removed", false, "reason", "Task not found", "taskId", id));
    }

    @GetMapping("/scheduler/{execution}/tasks/state")
    public ResponseEntity<?> getTaskStates(@PathVariable String execution,
                                           @RequestParam(name = "ids") String idsCsv) {
        return ResponseEntity.ok(service.getTaskStates(execution, idsCsv));
    }

    @GetMapping("/scheduler/{execution}/tasks/summary")
    public ResponseEntity<?> summarizeTasks(@PathVariable String execution,
                                            @RequestParam(name="ids", required=false) String idsCsv) {
        if (idsCsv == null || idsCsv.isBlank()) {
            return ResponseEntity.badRequest().body("Provide ids=<comma separated> until Scheduler exposes a public listing.");
        }
        return ResponseEntity.ok(service.getTaskStates(execution, idsCsv));
    }

    /* =============================================================
       NODE ENDPOINTS
       ============================================================= */

    @PostMapping("/cluster/node")
    public ResponseEntity<?> createOrUpdateNode(@RequestBody CreateNodeRequest req) {
        return ResponseEntity.ok(
                service.createOrUpdateNode(
                        req.name(),
                        req.cpu(),
                        req.memory(),
                        req.labels()
                )
        );
    }

    @DeleteMapping("/cluster/node/{name}")
    public ResponseEntity<?> deleteNode(@PathVariable String name) {
        boolean deleted = service.deleteNode(name);
        return deleted
                ? ResponseEntity.ok(Map.of("deleted", name))
                : ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("deleted", false, "node", name));
    }

    @PatchMapping("/cluster/node/{name}/labels")
    public ResponseEntity<?> patchNodeLabels(@PathVariable String name,
                                             @RequestBody PatchNodeLabelsRequest req) {
        return ResponseEntity.ok(service.patchNodeLabels(name, req.labels()));
    }

    /* =============================================================
       POD ENDPOINTS
       ============================================================= */

    @PostMapping("/cluster/pod")
    public ResponseEntity<?> createPod(@RequestBody CreatePodRequest req) {
        return ResponseEntity.ok(
                service.createPod(
                        req.name(),
                        req.namespace(),
                        req.nodeName(),
                        req.image(),
                        req.command(),
                        req.ip(),
                        req.nodeSelector(),
                        req.labels(),
                        req.cpuRequest(),
                        req.memoryRequest()
                )
        );
    }

    @PostMapping("/cluster/pods/{namespace}")
    public ResponseEntity<?> deletePods(@PathVariable String namespace,
                                       @RequestBody List<String> names) {
        for (String name: names){
            boolean deleted = service.deletePod(namespace, name);
            if (!deleted) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("deleted", false, "pod", name));
            }
        }
        return ResponseEntity.ok(Map.of("deleted", names));
    }

    @DeleteMapping("/cluster/reset")
    public ResponseEntity<?> resetCluster() {
        return ResponseEntity.ok(service.resetCluster());
    }

    /* =============================================================
       CLUSTER INFO
       ============================================================= */

    @GetMapping("/cluster/pods/count")
    public ResponseEntity<?> countPods() {
        return ResponseEntity.ok(Map.of("pods", service.countPods()));
    }

    @GetMapping("/cluster/pods")
    public ResponseEntity<?> listPods() {
        return ResponseEntity.ok(service.listPods());
    }

    @GetMapping("/cluster/nodes/count")
    public ResponseEntity<?> countNodes() {
        return ResponseEntity.ok(Map.of("nodes", service.countNodes()));
    }

    @GetMapping("/cluster/nodes")
    public ResponseEntity<?> listNodes() {
        return ResponseEntity.ok(service.listNodes());
    }

    @GetMapping("/cluster/snapshot")
    public ResponseEntity<?> snapshot(
            @RequestParam(name="executions", required = false) String executionsCsv,
            @RequestParam(name="taskIds", required = false) String taskIdsCsv
    ) {
        return ResponseEntity.ok(service.snapshot(executionsCsv, taskIdsCsv));
    }

    /* =============================================================
       TRACKING
       ============================================================= */

    @PostMapping("/scheduler/{execution}/track")
    public ResponseEntity<?> trackExecution(@PathVariable String execution) {
        service.trackExecution(execution);
        return ResponseEntity.ok(Map.of("tracked", execution));
    }

    @GetMapping("/scheduler/tracked")
    public ResponseEntity<?> listTrackedExecutions() {
        Set<String> list = service.listTrackedExecutions();
        return ResponseEntity.ok(list);
    }

    /* =============================================================
       ERROR HANDLING
       ============================================================= */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<?> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", ex.getMessage(),
                "type", "IllegalArgumentException"
        ));
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<?> handleNotFound(NoSuchElementException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                "error", ex.getMessage(),
                "type", "NoSuchElement"
        ));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<?> handleIllegalState(IllegalStateException ex) {
        log.error("Illegal state", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "error", ex.getMessage(),
                "type", "IllegalState"
        ));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleGeneric(Exception ex) {
        log.error("Unhandled exception in RestClusterInterface", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "error", ex.getMessage(),
                "type", ex.getClass().getSimpleName()
        ));
    }
}