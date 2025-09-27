package cws.k8s.scheduler.model;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

public class BatchTaskConfig {
    public int id;

    @JsonUnwrapped
    public TaskConfig config;
}
