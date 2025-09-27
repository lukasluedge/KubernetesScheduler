package cws.k8s.scheduler.local;



import cws.k8s.scheduler.client.CWSKubernetesClient;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.Scheduler;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class AdminClusterService {

    private final CWSKubernetesClient client;

    // In-memory tracking (moved from controller)
    private final Set<String> trackedExecutions = ConcurrentHashMap.newKeySet();

    // ---------------- Scheduler access (was reflection in controller) ----------------
    @SuppressWarnings("unchecked")
    private Map<String, Scheduler> schedulerHolder() {
        try {
            Class<?> restController = Class.forName("cws.k8s.scheduler.rest.SchedulerRestController");
            Field f = restController.getDeclaredField("schedulerHolder");
            f.setAccessible(true);
            return (Map<String, Scheduler>) f.get(null);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to access schedulerHolder via reflection. Consider adding a public getter.", e);
        }
    }

    private Scheduler getSchedulerOrNull(String execution) {
        return schedulerHolder().get(execution);
    }

    private Scheduler getSchedulerOrThrow(String execution) {
        Scheduler s = getSchedulerOrNull(execution);
        if (s == null) {
            throw new IllegalArgumentException("No scheduler for execution: " + execution);
        }
        return s;
    }

    // ---------------- Tasks ----------------
    public Map<String, Object> addTask(String execution,
                                       int id,
                                       String task,
                                       String name,
                                       String runName,
                                       String workDir) {
        Scheduler scheduler = getSchedulerOrThrow(execution);

        if (id < 0) {
            throw new IllegalArgumentException("Task id must be >= 0");
        }
        if (task == null) {
            throw new IllegalArgumentException("Field 'task' required");
        }

        TaskConfig conf = new TaskConfig(task, name, workDir, runName);
        scheduler.addTask(id, conf);
        log.info("Added task id={} (task={}) to execution {}", id, task, execution);
        return Map.of(
                "execution", execution,
                "taskId", id,
                "taskName", task,
                "accepted", true,
                "timestamp", Instant.now().toString()
        );
    }

    public boolean removeTask(String execution, int id) {
        Scheduler scheduler = getSchedulerOrThrow(execution);
        return scheduler.removeTask(id);
    }

    public Map<String, Object> getTaskStates(String execution, String idsCsv) {
        Scheduler scheduler = getSchedulerOrThrow(execution);
        String[] parts = idsCsv.split(",");
        Map<String, Object> result = new LinkedHashMap<>();
        for (String p : parts) {
            String token = p.trim();
            if (token.isEmpty()) continue;
            try {
                int tid = Integer.parseInt(token);
                Object state = scheduler.getTaskState(tid);
                result.put(token, state == null ? "UNKNOWN" : state);
            } catch (NumberFormatException nfe) {
                result.put(token, "INVALID_ID");
            }
        }
        return result;
    }

    // ---------------- Nodes ----------------
    public Map<String, Object> createOrUpdateNode(String name,
                                                  String cpu,
                                                  String memory,
                                                  Map<String, String> labels) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Node name required");
        }
        String cpuVal = cpu == null ? "1" : cpu;
        String memVal = memory == null ? "1Gi" : memory;

        Map<String, Quantity> alloc = Map.of(
                "cpu", new Quantity(cpuVal),
                "memory", new Quantity(memVal)
        );
        Map<String, Quantity> capacity = alloc;

        Map<String, String> effectiveLabels = new HashMap<>();
        if (labels != null) effectiveLabels.putAll(labels);
        effectiveLabels.putIfAbsent("kubernetes.io/hostname", name);
        effectiveLabels.putIfAbsent("kubernetes.io/os", "linux");
        effectiveLabels.putIfAbsent("kubernetes.io/arch", "amd64");

        Node existing = client.nodes().withName(name).get();

        if (existing != null) {
            // Update existing node's resources and labels without deleting/recreating
            if (existing.getStatus() == null) {
                existing.setStatus(new NodeStatus());
            }
            existing.getStatus().setAllocatable(alloc);
            existing.getStatus().setCapacity(capacity);

            // Merge/overwrite labels: keep existing, override with provided
            Map<String, String> lbl = existing.getMetadata().getLabels();
            if (lbl == null) lbl = new HashMap<>();
            lbl.putAll(effectiveLabels);
            existing.getMetadata().setLabels(lbl);

            client.nodes().resource(existing).update();
            log.info("Updated mock node {} (cpu={}, memory={})", name, cpuVal, memVal);
            return Map.of("updated", name, "cpu", cpuVal, "memory", memVal);
        }
        // Node does not exist; create as before
        NodeBuilder nb = new NodeBuilder()
                .withNewMetadata()
                .withName(name)
                .withLabels(effectiveLabels)
                .endMetadata()
                .withNewSpec()
                .withUnschedulable(false)
                .endSpec()
                .withNewStatus()
                .withAllocatable(alloc)
                .withCapacity(capacity)
                .addNewCondition()
                .withType("Ready")
                .withStatus("True")
                .withReason("Mock")
                .withMessage("Mock node is ready")
                .endCondition()
                .endStatus();

        Node node = nb.build();
        client.nodes().resource(node).create();
        log.info("Created mock node {}", name);
        return Map.of("created", name, "cpu", cpuVal, "memory", memVal);

    }

    public boolean deleteNode(String name) {
        var res = client.nodes().withName(name).delete();
        return res != null && !res.isEmpty();
    }

    public Map<String, Object> patchNodeLabels(String name, Map<String, String> labels) {
        if (labels == null || labels.isEmpty()) {
            throw new IllegalArgumentException("labels map required");
        }
        Node node = client.nodes().withName(name).get();
        if (node == null) {
            throw new NoSuchElementException("Node not found: " + name);
        }
        Map<String, String> existing = node.getMetadata().getLabels();
        if (existing == null) existing = new HashMap<>();
        existing.putAll(labels);
        node.getMetadata().setLabels(existing);
        client.nodes().resource(node).update();
        return Map.of("node", name, "labels", existing);
    }

    // ---------------- Pods ----------------
    public Map<String, Object> createPod(String name,
                                         String namespace,
                                         String nodeName,
                                         String image,
                                         List<String> command,
                                         String ip,
                                         Map<String, String> nodeSelector,
                                         Map<String, String> labels,
                                         String cpuRequest,
                                         String memoryRequest) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Pod name required");
        }
        String ns = (namespace == null || namespace.isBlank()) ? "default" : namespace;
        String img = image == null ? "alpine:3.19" : image;

        ResourceRequirements reqs = null;
        if (cpuRequest != null || memoryRequest != null) {
            Map<String, Quantity> requests = new HashMap<>();
            if (cpuRequest != null) requests.put("cpu", new Quantity(cpuRequest));
            if (memoryRequest != null) requests.put("memory", new Quantity(memoryRequest));
            reqs = new ResourceRequirementsBuilder().withRequests(requests).build();
        }

        ContainerBuilder cb = new ContainerBuilder()
                .withName("main")
                .withImage(img);
        if (command != null && !command.isEmpty()) {
            cb = cb.withCommand(command);
        }
        if (reqs != null) {
            cb = cb.withResources(reqs);
        }

        PodBuilder pb = new PodBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(ns)
                .withLabels(labels == null ? Map.of() : labels)
                .endMetadata()
                .withNewSpec()
                .withNodeName(nodeName)
                .withNodeSelector(nodeSelector)
                .withContainers(cb.build())
                .endSpec()
                .withNewStatus()
                .withPhase("Running")
                .withPodIP(ip)
                .endStatus();

        Pod pod = pb.build();
        client.pods().inNamespace(ns).resource(pod).create();
        log.info("Created mock pod {} in ns={} (node={})", name, ns, nodeName);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("created", name);
        resp.put("namespace", ns);
        resp.put("node", nodeName); // erlaubt null in der Map
        return resp;

    }

    public boolean deletePod(String namespace, String name) {
        var res = client.pods().inNamespace(namespace).withName(name).delete();
        return res != null && !res.isEmpty();
    }

    // Pod aus dem Cluster abrufen (für PodWithAge etc.)
    public Pod getPod(String namespace, String name) {
        if (namespace == null || namespace.isBlank()) {
            namespace = "default";
        }
        return client.pods().inNamespace(namespace).withName(name).get();
    }

    // ---------------- Cluster info ----------------
    public int countPods() {
        return client.pods().inAnyNamespace().list().getItems().size();
    }

    public List<Map<String, Object>> listPods() {
        List<Pod> pods = client.pods().inAnyNamespace().list().getItems();
        return pods.stream().map(p -> {
            String node = p.getSpec() != null ? p.getSpec().getNodeName() : null;
            String phase = p.getStatus() != null ? p.getStatus().getPhase() : null;
            String ip = p.getStatus() != null ? p.getStatus().getPodIP() : null;
            return Map.<String, Object>of(
                    "name", p.getMetadata().getName(),
                    "namespace", p.getMetadata().getNamespace(),
                    "node", node != null ? node : "null",
                    "phase", phase != null ? phase : "null",
                    "ip", ip != null ? ip : "null"
            );
        }).collect(Collectors.toList());
    }

    public int countNodes() {
        return client.nodes().list().getItems().size();
    }

    public List<Map<String, Object>> listNodes() {
        List<Node> nodes = client.nodes().list().getItems();
        List<Map<String, Object>> list = new ArrayList<>();
        for (Node n : nodes) {
            Map<String, Quantity> alloc = n.getStatus() != null ? n.getStatus().getAllocatable() : null;
            Map<String, String> labels = n.getMetadata() != null ? n.getMetadata().getLabels() : null;
            list.add(Map.of(
                    "name", n.getMetadata().getName(),
                    "allocatable", formatQuantities(alloc),
                    "labels", labels == null ? Map.of() : labels
            ));
        }
        return list;
    }

    public Map<String, Object> snapshot(String executionsCsv, String taskIdsCsv) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("timestamp", Instant.now().toString());

        // Nodes
        List<Node> nodes = client.nodes().list().getItems();
        out.put("nodes", nodes.stream().map(n -> Map.of(
                "name", n.getMetadata().getName(),
                "labels", Optional.ofNullable(n.getMetadata().getLabels()).orElse(Map.of()),
                "allocatable", formatQuantities(n.getStatus() == null ? null : n.getStatus().getAllocatable())
        )).toList());

        // Pods
        List<Pod> pods = client.pods().inAnyNamespace().list().getItems();
        out.put("pods", pods.stream().map(p -> Map.of(
                "name", p.getMetadata().getName(),
                "namespace", p.getMetadata().getNamespace(),
                "node", p.getSpec() != null ? (p.getSpec().getNodeName() != null ? p.getSpec().getNodeName(): "null")  : "null",
                "phase", p.getStatus() != null ? p.getStatus().getPhase() : "null",
                "ip", p.getStatus() != null ? p.getStatus().getPodIP() : "null"
        )).toList());

        // Optional Task States
        if (executionsCsv != null && taskIdsCsv != null) {
            Map<String, Object> execMap = new LinkedHashMap<>();
            for (String ex : executionsCsv.split(",")) {
                String execution = ex.trim();
                if (execution.isEmpty()) continue;
                Scheduler scheduler = getSchedulerOrNull(execution);
                if (scheduler == null) {
                    execMap.put(execution, "NO_SCHEDULER");
                    continue;
                }
                Map<String, Object> states = new LinkedHashMap<>();
                for (String tid : taskIdsCsv.split(",")) {
                    String token = tid.trim();
                    if (token.isEmpty()) continue;
                    try {
                        int id = Integer.parseInt(token);
                        Object st = scheduler.getTaskState(id);
                        states.put(token, st == null ? "UNKNOWN" : st);
                    } catch (NumberFormatException nfe) {
                        states.put(token, "INVALID_ID");
                    }
                }
                execMap.put(execution, states);
            }
            out.put("taskStates", execMap);
        }

        return out;
    }

    public String resetCluster() {
//         1) Delete all Pods in all namespaces first (grace=0, background)
//        List<Pod> pods = client.pods().inAnyNamespace().list().getItems();
//        for (Pod pod : pods) {
//            client.pods()
//                    .inNamespace(pod.getMetadata().getNamespace())
//                    .withName(pod.getMetadata().getName())
//                    .withGracePeriod(0)
//                    .withPropagationPolicy(DeletionPropagation.BACKGROUND)
//                    .delete();
//        }
//         Wait until all pods are gone (up to ~3 seconds)
        long waitUntil = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < waitUntil) {
            if (client.pods().inAnyNamespace().list().getItems().isEmpty()) break;
            try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
//
//        // 2) Delete all Nodes after pods are gone
//        List<Node> nodes = client.nodes().list().getItems();
//        for (Node node : nodes) {
//            client.nodes().withName(node.getMetadata().getName())
//                    .withGracePeriod(0)
//                    .withPropagationPolicy(DeletionPropagation.BACKGROUND)
//                    .delete();
//        }
//        // Wait until all nodes are gone (up to ~3 seconds)
//        waitUntil = System.currentTimeMillis() + 3000;
//        while (System.currentTimeMillis() < waitUntil) {
//            if (client.nodes().list().getItems().isEmpty()) break;
//            try { Thread.sleep(100); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
//        }
        return "no reset done";
//        return "Reset complete";
    }

    // ---------------- Tracking ----------------
    public void trackExecution(String execution) {
        trackedExecutions.add(execution);
    }

    public Set<String> listTrackedExecutions() {
        return trackedExecutions;
    }

    // ---------------- Utilities ----------------
    private Map<String, String> formatQuantities(Map<String, Quantity> q) {
        if (q == null) return Map.of();
        Map<String, String> out = new TreeMap<>();
        q.forEach((k, v) -> out.put(k, v.getAmount() + (v.getFormat() != null ? v.getFormat() : "")));
        return out;
    }
}
