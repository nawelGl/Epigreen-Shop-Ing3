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
     * La requête répond immédiatement, le job tourne en arrière-plan.
     * 
     * @param workers Nombre d'executors Spark (3 ou 6)
     * @param mode Mode d'exécution : dev (1 mois) ou prod (6 mois)
     * @return Status RUNNING avec jobId
     */
    @PostMapping("/run")
    public ResponseEntity<Job2ExecutionStatus> runJob2(
            @RequestParam(defaultValue = "3") int workers,
            @RequestParam(defaultValue = "dev") String mode) {

        // Validation
        if (workers != 3 && workers != 6) {
            return ResponseEntity.badRequest().body(
                new Job2ExecutionStatus(
                    "JOB2", "ERROR", -1, null, null, null,
                    "Invalid workers parameter: must be 3 or 6", 
                    null, null, null, null, workers
                )
            );
        }

        if (!mode.equals("dev") && !mode.equals("prod")) {
            return ResponseEntity.badRequest().body(
                new Job2ExecutionStatus(
                    "JOB2", "ERROR", -1, null, null, null,
                    "Invalid mode parameter: must be 'dev' or 'prod'", 
                    null, null, null, null, workers
                )
            );
        }

        // Lancer le job en async
        job2ExecutionService.runJob2Async(workers, mode);

        // Réponse immédiate
        return ResponseEntity.accepted().body(
            new Job2ExecutionStatus(
                "JOB2", "RUNNING", null, null, null, null,
                String.format("Job2 started with %d workers in %s mode", workers, mode),
                null, null, null, null, workers
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
                    "JOB2", "NOT_RUN", null, null, null, null,
                    "Job2 has not been executed yet.",
                    null, null, null, null, null
                )
            );
        }

        return ResponseEntity.ok(status);
    }
}
