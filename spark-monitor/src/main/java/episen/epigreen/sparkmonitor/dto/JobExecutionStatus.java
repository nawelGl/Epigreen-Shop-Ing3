package episen.epigreen.sparkmonitor.dto;

import java.time.LocalDateTime;

public class JobExecutionStatus {

    private String jobName;
    private String status;           // RUNNING, SUCCESS, FAILED
    private Integer exitCode;
    private Long durationSeconds;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String message;

    // ===== Constructeur vide (important pour Jackson / JSON)
    public JobExecutionStatus() {
    }

    // ===== Constructeur complet
    public JobExecutionStatus(String jobName,
                              String status,
                              Integer exitCode,
                              Long durationSeconds,
                              LocalDateTime startTime,
                              LocalDateTime endTime,
                              String message) {
        this.jobName = jobName;
        this.status = status;
        this.exitCode = exitCode;
        this.durationSeconds = durationSeconds;
        this.startTime = startTime;
        this.endTime = endTime;
        this.message = message;
    }

    // ===== Getters & Setters

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getExitCode() {
        return exitCode;
    }

    public void setExitCode(Integer exitCode) {
        this.exitCode = exitCode;
    }

    public Long getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(Long durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}