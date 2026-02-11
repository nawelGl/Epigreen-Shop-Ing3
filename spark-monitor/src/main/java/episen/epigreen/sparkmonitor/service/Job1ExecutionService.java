package episen.epigreen.sparkmonitor.service;

import episen.epigreen.sparkmonitor.dto.JobExecutionStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class Job1ExecutionService {

    private static final Logger log = LoggerFactory.getLogger(Job1ExecutionService.class);

    private final MeterRegistry meterRegistry;

    // Valeurs "en mémoire" pour les gauges (Prometheus va juste lire ces valeurs)
    private final AtomicInteger lastExitCode = new AtomicInteger(0);
    private final AtomicLong lastRunTimestamp = new AtomicLong(0);

    @Value("${spark.ssh.host}")
    private String host;

    @Value("${spark.ssh.user}")
    private String user;

    @Value("${spark.ssh.port:22}")
    private int port;

    @Value("${spark.job1.remoteScript}")
    private String remoteScript;

    // Tag pour comparer (ex: workers=3 puis tu changes dans application.properties)
    @Value("${spark.job1.workers:3}")
    private String workers;

    public Job1ExecutionService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Ici on enregistre 2 gauges UNE SEULE FOIS.
     * Prometheus verra toujours ces 2 métriques, et leurs valeurs changent après chaque run.
     */
    @PostConstruct
    public void initGauges() {
        Gauge.builder("job_exit_code", lastExitCode, AtomicInteger::get)
                .tag("job", "job1")
                .tag("workers", workers)
                .register(meterRegistry);

        Gauge.builder("job_last_run_timestamp", lastRunTimestamp, AtomicLong::get)
                .tag("job", "job1")
                .tag("workers", workers)
                .register(meterRegistry);
    }

    public JobExecutionStatus runJob1() {
        // ---- logs lisibles côté microservice ----
        log.info("[JOB1] Trigger requested (ssh to {}@{}:{}) workers={}", user, host, port, workers);
        log.info("[JOB1] Remote script: {}", remoteScript);

        LocalDateTime start = LocalDateTime.now();

        // Timer micrometer (mesure la durée totale du run côté microservice)
        Timer.Sample timerSample = Timer.start(meterRegistry);

        int exitCode = -1;
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();

        try {
            // ---- 1) Construire la commande SSH ----
            // BatchMode=yes : si la clé SSH n'est pas OK => échec direct (pas de prompt mot de passe)
            // StrictHostKeyChecking=no : évite des prompts "Are you sure..." pendant la démo
            String sshTarget = user + "@" + host;

            List<String> cmd = new ArrayList<>();
            cmd.add("ssh");
            cmd.add("-p");
            cmd.add(String.valueOf(port));
            cmd.add("-o");
            cmd.add("BatchMode=yes");
            cmd.add("-o");
            cmd.add("StrictHostKeyChecking=no");
            cmd.add(sshTarget);
            cmd.add("bash " + remoteScript);

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(false);

            // ---- 2) Exécuter ----
            long t0 = System.nanoTime();
            Process p = pb.start();

            // ---- 3) Lire STDOUT ----
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stdout.append(line).append("\n");
                }
            }

            // ---- 4) Lire STDERR ----
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stderr.append(line).append("\n");
                }
            }

            // ---- 5) Attendre la fin + exit code ----
            exitCode = p.waitFor();
            long t1 = System.nanoTime();

            long durationSeconds = Duration.ofNanos(t1 - t0).toSeconds();
            LocalDateTime end = LocalDateTime.now();

            boolean success = (exitCode == 0);

            // ---- METRICS (Micrometer) ----
            // 1) Durée
            timerSample.stop(
                    Timer.builder("job_duration_seconds")
                            .tag("job", "job1")
                            .tag("workers", workers)
                            .register(meterRegistry)
            );

            // 2) Success / Failure
            if (success) {
                Counter.builder("job_success_total")
                        .tag("job", "job1")
                        .tag("workers", workers)
                        .register(meterRegistry)
                        .increment();
            } else {
                Counter.builder("job_failure_total")
                        .tag("job", "job1")
                        .tag("workers", workers)
                        .register(meterRegistry)
                        .increment();
            }

            // 3) Gauges 
            lastExitCode.set(exitCode);
            lastRunTimestamp.set(Instant.now().getEpochSecond());

            // ---- Réponse HTTP ----
            String status = success ? "SUCCESS" : "FAILED";
            String msg = success ? "Job executed successfully" : "Job failed (exitCode=" + exitCode + ")";

            log.info("[JOB1] Finished status={} exitCode={} duration={}s", status, exitCode, durationSeconds);

            return new JobExecutionStatus(
                    "JOB1",
                    status,
                    exitCode,
                    durationSeconds,
                    start,
                    end,
                    msg + "\n" + buildMessage(stdout.toString(), stderr.toString())
            );

        } catch (Exception e) {
            LocalDateTime end = LocalDateTime.now();
            log.error("[JOB1] Exception while running job1", e);

            // Durée 
            timerSample.stop(
                    Timer.builder("job_duration_seconds")
                            .tag("job", "job1")
                            .tag("workers", workers)
                            .register(meterRegistry)
            );

            // Failure counter
            Counter.builder("job_failure_total")
                    .tag("job", "job1")
                    .tag("workers", workers)
                    .register(meterRegistry)
                    .increment();

            // Gauges
            lastExitCode.set(exitCode);
            lastRunTimestamp.set(Instant.now().getEpochSecond());

            return new JobExecutionStatus(
                    "JOB1",
                    "FAILED",
                    exitCode,
                    null,
                    start,
                    end,
                    "Exception: " + e.getMessage()
            );
        }
    }

    /**
     * Pour éviter une réponse JSON énorme, on renvoie seulement le "tail" des logs.
     */
    private String buildMessage(String stdout, String stderr) {
        int maxChars = 4000;

        String out = (stdout == null) ? "" : stdout.trim();
        String err = (stderr == null) ? "" : stderr.trim();

        if (out.length() > maxChars) out = out.substring(out.length() - maxChars);
        if (err.length() > maxChars) err = err.substring(err.length() - maxChars);

        StringBuilder sb = new StringBuilder();
        if (!out.isBlank()) sb.append("---- STDOUT (tail) ----\n").append(out).append("\n");
        if (!err.isBlank()) sb.append("---- STDERR (tail) ----\n").append(err).append("\n");
        if (sb.isEmpty()) sb.append("(no stdout/stderr captured)");
        return sb.toString();
    }
}