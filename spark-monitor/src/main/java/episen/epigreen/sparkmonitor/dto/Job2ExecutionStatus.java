package episen.epigreen.sparkmonitor.dto;

import java.time.LocalDateTime;

public class Job2ExecutionStatus {

    private String jobName;
    private String status;           // RUNNING, SUCCESS, FAILED
    private Integer exitCode;
    private Long durationSeconds;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String message;
    
    // Métriques spécifiques Job2
    private Long rowsRead;
    private Long rowsFiltered;
    private Long rowsWritten;
    private Integer shufflePartitions;
    private Integer numExecutors;

    // Constructeur vide
    public Job2ExecutionStatus() {
    }

    // Constructeur complet
    public Job2ExecutionStatus(String jobName,
                              String status,
                              Integer exitCode,
                              Long durationSeconds,
                              LocalDateTime startTime,
                              LocalDateTime endTime,
                              String message,
                              Long rowsRead,
                              Long rowsFiltered,
                              Long rowsWritten,
                              Integer shufflePartitions,
                              Integer numExecutors) {
        this.jobName = jobName;
        this.status = status;
        this.exitCode = exitCode;
        this.durationSeconds = durationSeconds;
        this.startTime = startTime;
        this.endTime = endTime;
        this.message = message;
        this.rowsRead = rowsRead;
        this.rowsFiltered = rowsFiltered;
        this.rowsWritten = rowsWritten;
        this.shufflePartitions = shufflePartitions;
        this.numExecutors = numExecutors;
    }

    // Getters & Setters

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

    public Long getRowsRead() {
        return rowsRead;
    }

    public void setRowsRead(Long rowsRead) {
        this.rowsRead = rowsRead;
    }

    public Long getRowsFiltered() {
        return rowsFiltered;
    }

    public void setRowsFiltered(Long rowsFiltered) {
        this.rowsFiltered = rowsFiltered;
    }

    public Long getRowsWritten() {
        return rowsWritten;
    }

    public void setRowsWritten(Long rowsWritten) {
        this.rowsWritten = rowsWritten;
    }

    public Integer getShufflePartitions() {
        return shufflePartitions;
    }

    public void setShufflePartitions(Integer shufflePartitions) {
        this.shufflePartitions = shufflePartitions;
    }

    public Integer getNumExecutors() {
        return numExecutors;
    }

    public void setNumExecutors(Integer numExecutors) {
        this.numExecutors = numExecutors;
    }
}
