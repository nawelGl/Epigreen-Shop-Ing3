package episen.epigreen.sparkmonitor.dto;

import java.time.LocalDateTime;

public class Job2ExecutionStatus {

    private String jobName;
    private String status;           // RUNNING, SUCCESS, FAILED
    private Integer exitCode;

    // Temps global
    private Double totalDurationSeconds;

    // Temps par étape métier
    private Double readDurationSeconds;
    private Double aggregationWarehouseDurationSeconds;
    private Double hdfsWriteDurationSeconds;
    private Double aggregationProductDurationSeconds;
    private Double postgresWriteDurationSeconds;
    private Double postgresUpdateDurationSeconds;

    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String message;

    private Integer numExecutors;

    // Constructeur vide
    public Job2ExecutionStatus() {}

    // Constructeur complet
    public Job2ExecutionStatus(
            String jobName,
            String status,
            Integer exitCode,
            Double totalDurationSeconds,
            Double readDurationSeconds,
            Double aggregationWarehouseDurationSeconds,
            Double hdfsWriteDurationSeconds,
            Double aggregationProductDurationSeconds,
            Double postgresWriteDurationSeconds,
            Double postgresUpdateDurationSeconds,
            LocalDateTime startTime,
            LocalDateTime endTime,
            String message,
            Integer numExecutors) {

        this.jobName = jobName;
        this.status = status;
        this.exitCode = exitCode;
        this.totalDurationSeconds = totalDurationSeconds;
        this.readDurationSeconds = readDurationSeconds;
        this.aggregationWarehouseDurationSeconds = aggregationWarehouseDurationSeconds;
        this.hdfsWriteDurationSeconds = hdfsWriteDurationSeconds;
        this.aggregationProductDurationSeconds = aggregationProductDurationSeconds;
        this.postgresWriteDurationSeconds = postgresWriteDurationSeconds;
        this.postgresUpdateDurationSeconds = postgresUpdateDurationSeconds;
        this.startTime = startTime;
        this.endTime = endTime;
        this.message = message;
        this.numExecutors = numExecutors;
    }

    // =======================
    // Getters & Setters
    // =======================

    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Integer getExitCode() { return exitCode; }
    public void setExitCode(Integer exitCode) { this.exitCode = exitCode; }

    public Double getTotalDurationSeconds() { return totalDurationSeconds; }
    public void setTotalDurationSeconds(Double totalDurationSeconds) {
        this.totalDurationSeconds = totalDurationSeconds;
    }

    public Double getReadDurationSeconds() { return readDurationSeconds; }
    public void setReadDurationSeconds(Double readDurationSeconds) {
        this.readDurationSeconds = readDurationSeconds;
    }

    public Double getAggregationWarehouseDurationSeconds() {
        return aggregationWarehouseDurationSeconds;
    }
    public void setAggregationWarehouseDurationSeconds(Double aggregationWarehouseDurationSeconds) {
        this.aggregationWarehouseDurationSeconds = aggregationWarehouseDurationSeconds;
    }

    public Double getHdfsWriteDurationSeconds() { return hdfsWriteDurationSeconds; }
    public void setHdfsWriteDurationSeconds(Double hdfsWriteDurationSeconds) {
        this.hdfsWriteDurationSeconds = hdfsWriteDurationSeconds;
    }

    public Double getAggregationProductDurationSeconds() {
        return aggregationProductDurationSeconds;
    }
    public void setAggregationProductDurationSeconds(Double aggregationProductDurationSeconds) {
        this.aggregationProductDurationSeconds = aggregationProductDurationSeconds;
    }

    public Double getPostgresWriteDurationSeconds() {
        return postgresWriteDurationSeconds;
    }
    public void setPostgresWriteDurationSeconds(Double postgresWriteDurationSeconds) {
        this.postgresWriteDurationSeconds = postgresWriteDurationSeconds;
    }

    public Double getPostgresUpdateDurationSeconds() {
        return postgresUpdateDurationSeconds;
    }
    public void setPostgresUpdateDurationSeconds(Double postgresUpdateDurationSeconds) {
        this.postgresUpdateDurationSeconds = postgresUpdateDurationSeconds;
    }

    public LocalDateTime getStartTime() { return startTime; }
    public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }

    public LocalDateTime getEndTime() { return endTime; }
    public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Integer getNumExecutors() { return numExecutors; }
    public void setNumExecutors(Integer numExecutors) {
        this.numExecutors = numExecutors;
    }
}