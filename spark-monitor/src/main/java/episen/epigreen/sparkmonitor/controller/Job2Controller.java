package episen.epigreen.sparkmonitor.controller;

import episen.epigreen.sparkmonitor.dto.Job2ExecutionStatus;
import episen.epigreen.sparkmonitor.service.Job2ExecutionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/job2")
public class Job2Controller {

    private final Job2ExecutionService job2ExecutionService;

    public Job2Controller(Job2ExecutionService job2ExecutionService) {
        this.job2ExecutionService = job2ExecutionService;
    }

    /**
     * Lance Job2 en mode asynchrone avec le nombre de workers spécifié.
     * Répond immédiatement (HTTP 202), le job tourne en arrière-plan.
     *
     * @param workers Nombre d'executors Spark (ex: 1, 3, 6)
     * @param mode Mode d'exécution : dev (1 mois) ou prod (6 mois)
     */
    @PostMapping("/run")
    public ResponseEntity<Job2ExecutionStatus> runJob2(
            @RequestParam(defaultValue = "3") int workers,
            @RequestParam(defaultValue = "dev") String mode) {

        if (workers <= 0) {
            return ResponseEntity.badRequest().body(
                    new Job2ExecutionStatus(
                            "JOB2",
                            "ERROR",
                            -1,
                            null, null, null, null, null, null, null,
                            null,
                            null,
                            "Invalid workers parameter: must be > 0",
                            workers
                    )
            );
        }

        if (!mode.equals("dev") && !mode.equals("prod")) {
            return ResponseEntity.badRequest().body(
                    new Job2ExecutionStatus(
                            "JOB2",
                            "ERROR",
                            -1,
                            null, null, null, null, null, null, null,
                            null,
                            null,
                            "Invalid mode parameter: must be 'dev' or 'prod'",
                            workers
                    )
            );
        }

        job2ExecutionService.runJob2Async(workers, mode);

        return ResponseEntity.accepted().body(
                new Job2ExecutionStatus(
                        "JOB2",
                        "RUNNING",
                        null,
                        null, null, null, null, null, null, null,
                        null,
                        null,
                        String.format("Job2 started with %d workers in %s mode", workers, mode),
                        workers
                )
        );
    }

    /**
     * Retourne le dernier statut connu du Job2.
     */
    @GetMapping("/status")
    public ResponseEntity<Job2ExecutionStatus> getLastStatus() {
        Job2ExecutionStatus status = job2ExecutionService.getLastStatus();

        if (status == null) {
            return ResponseEntity.ok(
                    new Job2ExecutionStatus(
                            "JOB2",
                            "NOT_RUN",
                            null,
                            null, null, null, null, null, null, null,
                            null,
                            null,
                            "Job2 has not been executed yet.",
                            null
                    )
            );
        }

        return ResponseEntity.ok(status);
    }

   

    
}