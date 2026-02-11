package episen.epigreen.sparkmonitor.controller;

import episen.epigreen.sparkmonitor.dto.JobExecutionStatus;
import episen.epigreen.sparkmonitor.service.Job1ExecutionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/job1")
public class Job1Controller {

    private final Job1ExecutionService job1ExecutionService;

    // On garde le dernier résultat en mémoire
    private JobExecutionStatus lastStatus;

    public Job1Controller(Job1ExecutionService job1ExecutionService) {
        this.job1ExecutionService = job1ExecutionService;
    }

    /**
     * Lance Job1 (bloquant : la requête attend la fin du job).
     */
    @PostMapping("/run")
    public ResponseEntity<JobExecutionStatus> runJob1() {

        JobExecutionStatus status = job1ExecutionService.runJob1();

        // On mémorise le résultat
        this.lastStatus = status;

        if ("SUCCESS".equalsIgnoreCase(status.getStatus())) {
            return ResponseEntity.ok(status);
        }

        return ResponseEntity.status(500).body(status);
    }

    /**
     * Retourne le dernier résultat connu.
     */
    @GetMapping("/status")
    public ResponseEntity<JobExecutionStatus> getLastStatus() {

        if (lastStatus == null) {
            return ResponseEntity.ok(
                    new JobExecutionStatus(
                            "JOB1",
                            "NOT_RUN",
                            null,
                            null,
                            null,
                            null,
                            "Job has not been executed yet."
                    )
            );
        }

        return ResponseEntity.ok(lastStatus);
    }
}