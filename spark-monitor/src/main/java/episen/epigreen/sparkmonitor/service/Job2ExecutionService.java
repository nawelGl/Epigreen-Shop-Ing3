package episen.epigreen.sparkmonitor.service;

import episen.epigreen.sparkmonitor.dto.Job2ExecutionStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class Job2ExecutionService {

    private static final Logger log = LoggerFactory.getLogger(Job2ExecutionService.class);

    private final MeterRegistry meterRegistry;

    // Statut courant en mémoire (thread-safe)
    private final AtomicReference<Job2ExecutionStatus> lastStatus = new AtomicReference<>();

    // Valeurs pour les gauges Prometheus
    private final AtomicInteger lastExitCode = new AtomicInteger(0);
    private final AtomicLong lastRunTimestamp = new AtomicLong(0);
    private final AtomicLong lastRowsRead = new AtomicLong(0);
    private final AtomicLong lastRowsWritten = new AtomicLong(0);
    private final AtomicLong lastRowsFiltered = new AtomicLong(0);
    private final AtomicLong lastDuration = new AtomicLong(0);

    @Value("${spark.ssh.host}")
    private String host;

    @Value("${spark.ssh.user}")
    private String user;

    @Value("${spark.ssh.port:22}")
    private int port;

    @Value("${spark.job2.remoteScript}")
    private String remoteScript;

    public Job2ExecutionService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Enregistre les gauges au démarrage.
     * Prometheus lit ces valeurs en continu.
     */
    @PostConstruct
    public void initGauges() {
        Gauge.builder("job2_exit_code", lastExitCode, AtomicInteger::get)
                .tag("job", "job2")
                .description("Last exit code of Job2")
                .register(meterRegistry);

        Gauge.builder("job2_last_run_timestamp", lastRunTimestamp, AtomicLong::get)
                .tag("job", "job2")
                .description("Timestamp of last Job2 run")
                .register(meterRegistry);

        Gauge.builder("job2_rows_read", lastRowsRead, AtomicLong::get)
                .tag("job", "job2")
                .description("Rows read from curated_base")
                .register(meterRegistry);

        Gauge.builder("job2_rows_written", lastRowsWritten, AtomicLong::get)
                .tag("job", "job2")
                .description("Rows written to curated_final")
                .register(meterRegistry);

        Gauge.builder("job2_rows_filtered", lastRowsFiltered, AtomicLong::get)
                .tag("job", "job2")
                .description("Rows filtered (data quality)")
                .register(meterRegistry);

        Gauge.builder("job2_duration_seconds", lastDuration, AtomicLong::get)
                .tag("job", "job2")
                .description("Duration of last Job2 execution")
                .register(meterRegistry);
    }

    /**
     * Lance le Job2 en mode asynchrone.
     * La méthode retourne immédiatement.
     */
    @Async
    public void runJob2Async(int workers, String mode) {
        log.info("[JOB2] ========================================");
        log.info("[JOB2] Async execution started");
        log.info("[JOB2] workers={} mode={}", workers, mode);
        log.info("[JOB2] ========================================");

        lastStatus.set(new Job2ExecutionStatus(
            "JOB2",
            "RUNNING",
            null,
            null,
            LocalDateTime.now(),
            null,
            String.format("Job2 running with %d workers in %s mode", workers, mode),
            null, null, null, null,
            workers
    ));

    Job2ExecutionStatus status = runJob2Sync(workers, mode);
    lastStatus.set(status);

        log.info("[JOB2] Async execution finished | status={}", status.getStatus());
    }

    /**
     * Exécution synchrone du job.
     * Parse les métriques du stdout SSH.
     */
    private Job2ExecutionStatus runJob2Sync(int workers, String mode) {
        String workersTag = String.valueOf(workers);
        LocalDateTime start = LocalDateTime.now();

        // Timer Micrometer
        Timer.Sample timerSample = Timer.start(meterRegistry);

        int exitCode = -1;
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();

        // Métriques parsées du stdout
        Long rowsRead = null;
        Long rowsFiltered = null;
        Long rowsWritten = null;
        Integer shufflePartitions = null;
        Long elapsedTotal = null;

        try {
            // ---- 1) Commande SSH ----
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
            
            // Variables d'environnement pour le script bash
            String bashCmd = String.format("MODE=%s NUM_EXECUTORS=%d bash %s", mode, workers, remoteScript);
            cmd.add(bashCmd);

            log.info("[JOB2] SSH command: {}", String.join(" ", cmd));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(false);

            // ---- 2) Exécution ----
            long t0 = System.nanoTime();
            Process p = pb.start();

            // ---- 3) Lecture STDOUT (ligne par ligne pour logs temps réel) ----
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stdout.append(line).append("\n");
                    
                    // Log en temps réel pour debug
                    if (line.contains("[JOB2]")) {
                        log.info(line);
                    }
                }
            }

            // ---- 4) Lecture STDERR ----
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stderr.append(line).append("\n");
                }
            }

            // ---- 5) Attendre la fin ----
            exitCode = p.waitFor();
            long t1 = System.nanoTime();

            long durationSeconds = Duration.ofNanos(t1 - t0).toSeconds();
            LocalDateTime end = LocalDateTime.now();

            boolean success = (exitCode == 0);

            // ---- 6) Parser les métriques du stdout ----
            String stdoutStr = stdout.toString();
            rowsRead = parseLongMetric(stdoutStr, "rows_read");
            rowsFiltered = parseLongMetric(stdoutStr, "rows_filtered");
            rowsWritten = parseLongMetric(stdoutStr, "rows_written");
            shufflePartitions = parseIntMetric(stdoutStr, "shuffle_partitions");
            elapsedTotal = parseLongMetric(stdoutStr, "elapsed_total_seconds");

            log.info("[JOB2] ========================================");
            log.info("[JOB2] Metrics parsed from stdout:");
            log.info("[JOB2] rows_read={}", rowsRead);
            log.info("[JOB2] rows_filtered={}", rowsFiltered);
            log.info("[JOB2] rows_written={}", rowsWritten);
            log.info("[JOB2] shuffle_partitions={}", shufflePartitions);
            log.info("[JOB2] elapsed_total_seconds={}", elapsedTotal);
            log.info("[JOB2] ========================================");

            // ---- 7) Métriques Micrometer ----
            timerSample.stop(
                    Timer.builder("job2_duration_seconds")
                            .tag("job", "job2")
                            .tag("workers", workersTag)
                            .tag("mode", mode)
                            .register(meterRegistry)
            );

            if (success) {
                Counter.builder("job2_success_total")
                        .tag("job", "job2")
                        .tag("workers", workersTag)
                        .tag("mode", mode)
                        .register(meterRegistry)
                        .increment();
            } else {
                Counter.builder("job2_failure_total")
                        .tag("job", "job2")
                        .tag("workers", workersTag)
                        .tag("mode", mode)
                        .register(meterRegistry)
                        .increment();
            }

            // Mise à jour des gauges
            lastExitCode.set(exitCode);
            lastRunTimestamp.set(Instant.now().getEpochSecond());
            if (rowsRead != null) lastRowsRead.set(rowsRead);
            if (rowsWritten != null) lastRowsWritten.set(rowsWritten);
            if (rowsFiltered != null) lastRowsFiltered.set(rowsFiltered);
            if (elapsedTotal != null) lastDuration.set(elapsedTotal);

            // ---- 8) Retour ----
            String status = success ? "SUCCESS" : "FAILED";
            String msg = success ? "Job2 executed successfully" : "Job2 failed (exitCode=" + exitCode + ")";

            return new Job2ExecutionStatus(
                    "JOB2",
                    status,
                    exitCode,
                    durationSeconds,
                    start,
                    end,
                    msg + "\n" + buildTailMessage(stdoutStr, stderr.toString()),
                    rowsRead,
                    rowsFiltered,
                    rowsWritten,
                    shufflePartitions,
                    workers
            );

        } catch (Exception e) {
            LocalDateTime end = LocalDateTime.now();
            log.error("[JOB2] Exception during execution", e);

            timerSample.stop(
                    Timer.builder("job2_duration_seconds")
                            .tag("job", "job2")
                            .tag("workers", workersTag)
                            .tag("mode", mode)
                            .register(meterRegistry)
            );

            Counter.builder("job2_failure_total")
                    .tag("job", "job2")
                    .tag("workers", workersTag)
                    .tag("mode", mode)
                    .register(meterRegistry)
                    .increment();

            lastExitCode.set(exitCode);
            lastRunTimestamp.set(Instant.now().getEpochSecond());

            return new Job2ExecutionStatus(
                    "JOB2",
                    "FAILED",
                    exitCode,
                    null,
                    start,
                    end,
                    "Exception: " + e.getMessage(),
                    rowsRead,
                    rowsFiltered,
                    rowsWritten,
                    shufflePartitions,
                    workers
            );
        }
    }

    /**
     * Parse une métrique Long depuis le stdout (format: key=value).
     */
    private Long parseLongMetric(String stdout, String key) {
        Pattern pattern = Pattern.compile(key + "=(\\d+)");
        Matcher matcher = pattern.matcher(stdout);
        if (matcher.find()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                log.warn("[JOB2] Failed to parse {} as Long", key);
            }
        }
        return null;
    }

    /**
     * Parse une métrique Integer depuis le stdout.
     */
    private Integer parseIntMetric(String stdout, String key) {
        Pattern pattern = Pattern.compile(key + "=(\\d+)");
        Matcher matcher = pattern.matcher(stdout);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                log.warn("[JOB2] Failed to parse {} as Integer", key);
            }
        }
        return null;
    }

    /**
     * Construit un tail des logs .
     */
    private String buildTailMessage(String stdout, String stderr) {
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

    /**
     * Retourne le dernier statut connu.
     */
    public Job2ExecutionStatus getLastStatus() {
        return lastStatus.get();
    }
}
