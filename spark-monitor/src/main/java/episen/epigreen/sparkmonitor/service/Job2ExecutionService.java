package episen.epigreen.sparkmonitor.service;

import episen.epigreen.sparkmonitor.dto.Job2ExecutionStatus;
import io.micrometer.core.instrument.Counter;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class Job2ExecutionService {

    private static final Logger log = LoggerFactory.getLogger(Job2ExecutionService.class);

    private final MeterRegistry meterRegistry;

    private final AtomicReference<Job2ExecutionStatus> lastStatus = new AtomicReference<>();

    private final AtomicInteger lastExitCode = new AtomicInteger(0);
    private final AtomicLong lastRunTimestamp = new AtomicLong(0);
    private final AtomicReference<Process> runningProcess = new AtomicReference<>();

    // Gauges 
    private final AtomicReference<Double> lastTotal = new AtomicReference<>(0.0);
    private final AtomicReference<Double> lastRead = new AtomicReference<>(0.0);
    private final AtomicReference<Double> lastAggWarehouse = new AtomicReference<>(0.0);
    private final AtomicReference<Double> lastHdfsWrite = new AtomicReference<>(0.0);
    private final AtomicReference<Double> lastAggProduct = new AtomicReference<>(0.0);
    private final AtomicReference<Double> lastPgWrite = new AtomicReference<>(0.0);
    private final AtomicReference<Double> lastPgUpdate = new AtomicReference<>(0.0);

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

    @PostConstruct
    public void initGauges() {
        meterRegistry.gauge("job2_exit_code", lastExitCode);
        meterRegistry.gauge("job2_last_run_timestamp", lastRunTimestamp);

        meterRegistry.gauge("job2_step_total_seconds", lastTotal, v -> v.get() == null ? 0.0 : v.get());
        meterRegistry.gauge("job2_step_read_hdfs_seconds", lastRead, v -> v.get() == null ? 0.0 : v.get());
        meterRegistry.gauge("job2_step_agg_warehouse_seconds", lastAggWarehouse, v -> v.get() == null ? 0.0 : v.get());
        meterRegistry.gauge("job2_step_write_hdfs_seconds", lastHdfsWrite, v -> v.get() == null ? 0.0 : v.get());
        meterRegistry.gauge("job2_step_agg_product_seconds", lastAggProduct, v -> v.get() == null ? 0.0 : v.get());
        meterRegistry.gauge("job2_step_write_pg_seconds", lastPgWrite, v -> v.get() == null ? 0.0 : v.get());
        meterRegistry.gauge("job2_step_update_pg_seconds", lastPgUpdate, v -> v.get() == null ? 0.0 : v.get());
    }

    @Async
    public void runJob2Async(int workers, String mode) {
        log.info("[JOB2] Async execution started | workers={} mode={}", workers, mode);

        lastStatus.set(new Job2ExecutionStatus(
                "JOB2",
                "RUNNING",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                LocalDateTime.now(),
                null,
                "Job2 running",
                workers
        ));

        Job2ExecutionStatus status = runJob2Sync(workers, mode);
        lastStatus.set(status);

        log.info("[JOB2] Async execution finished | status={}", status.getStatus());
    }

    private Job2ExecutionStatus runJob2Sync(int workers, String mode) {

        LocalDateTime start = LocalDateTime.now();
        Timer.Sample timerSample = Timer.start(meterRegistry);

        int exitCode = -1;
        StringBuilder stdout = new StringBuilder();

        try {
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

            String bashCmd = String.format("MODE=%s NUM_EXECUTORS=%d bash %s", mode, workers, remoteScript);
            cmd.add(bashCmd);

            log.info("[JOB2] SSH command: {}", String.join(" ", cmd));

            ProcessBuilder pb = new ProcessBuilder(cmd);

            // ✅ fusionne stdout + stderr (donc tu ne perds aucune erreur)
            pb.redirectErrorStream(true);

            long t0 = System.nanoTime();
            Process p = pb.start();
            runningProcess.set(p);

            // ---- Lecture logs remote en temps réel (filtrée proprement) ----
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stdout.append(line).append("\n");

                    String lower = line.toLowerCase();

                    // ✅ logs métier
                    if (line.contains("[JOB2]")) {
                        log.info(line);
                    }
                    // ✅ erreurs visibles même si pas de [JOB2]
                    else if (lower.contains("exception") || lower.contains("error") || lower.contains("failed")) {
                        log.error("[JOB2-ERROR] {}", line);
                    }
                    // (optionnel) warnings
                    else if (lower.contains("warn") || lower.contains("warning")) {
                        log.warn("[JOB2-WARN] {}", line);
                    }
                }
            }

            exitCode = p.waitFor();
            long t1 = System.nanoTime();

            long durationSeconds = Duration.ofNanos(t1 - t0).toSeconds();
            LocalDateTime end = LocalDateTime.now();
            boolean success = (exitCode == 0);

            // ============================
            // PARSING MÉTRIQUES PERFORMANCE
            // ============================
            Map<String, Double> metrics = parseMetrics(stdout.toString());

            Double total = metrics.get("temps_total_traitement_secondes");
            Double read = metrics.get("temps_lecture_hdfs_secondes");
            Double aggWarehouse = metrics.get("temps_aggregation_warehouse_secondes");
            Double hdfsWrite = metrics.get("temps_ecriture_hdfs_secondes");
            Double aggProduct = metrics.get("temps_aggregation_produit_secondes");
            Double pgWrite = metrics.get("temps_ecriture_postgresql_secondes"); // ⚠️ ton script logge "postgresql" chez toi
            Double pgUpdate = metrics.get("temps_update_catalogue_secondes");    // ⚠️ idem

            if (total != null) lastTotal.set(total);
            if (read != null) lastRead.set(read);
            if (aggWarehouse != null) lastAggWarehouse.set(aggWarehouse);
            if (hdfsWrite != null) lastHdfsWrite.set(hdfsWrite);
            if (aggProduct != null) lastAggProduct.set(aggProduct);
            if (pgWrite != null) lastPgWrite.set(pgWrite);
            if (pgUpdate != null) lastPgUpdate.set(pgUpdate);

            // ---- Micrometer Timer ----
            timerSample.stop(
                    Timer.builder("job2_duration_seconds")
                            .tag("workers", String.valueOf(workers))
                            .tag("mode", mode)
                            .register(meterRegistry)
            );

            if (success) {
                Counter.builder("job2_success_total")
                        .tag("workers", String.valueOf(workers))
                        .tag("mode", mode)
                        .register(meterRegistry)
                        .increment();
            } else {
                Counter.builder("job2_failure_total")
                        .tag("workers", String.valueOf(workers))
                        .tag("mode", mode)
                        .register(meterRegistry)
                        .increment();
            }

            lastExitCode.set(exitCode);
            lastRunTimestamp.set(Instant.now().getEpochSecond());

            String msg = success
                    ? "Job2 executed successfully"
                    : "Job2 failed (exitCode=" + exitCode + ")";

            return new Job2ExecutionStatus(
                    "JOB2",
                    success ? "SUCCESS" : "FAILED",
                    exitCode,
                    total,
                    read,
                    aggWarehouse,
                    hdfsWrite,
                    aggProduct,
                    pgWrite,
                    pgUpdate,
                    start,
                    end,
                    msg,
                    workers
            );

        } catch (Exception e) {
            log.error("[JOB2] Exception during execution", e);

            lastExitCode.set(exitCode);
            lastRunTimestamp.set(Instant.now().getEpochSecond());

            return new Job2ExecutionStatus(
                    "JOB2",
                    "FAILED",
                    exitCode,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    start,
                    LocalDateTime.now(),
                    "Exception: " + e.getMessage(),
                    workers
            );
        } finally {
            runningProcess.set(null);
        }
    }

    private Map<String, Double> parseMetrics(String stdout) {
        Map<String, Double> metrics = new HashMap<>();

        Pattern pattern = Pattern.compile("metrique:([a-zA-Z0-9_]+)=([0-9.]+)");
        Matcher matcher = pattern.matcher(stdout);

        while (matcher.find()) {
            String key = matcher.group(1);
            Double value = Double.parseDouble(matcher.group(2));
            metrics.put(key, value);
        }

        return metrics;
    }

    public Job2ExecutionStatus getLastStatus() {
        return lastStatus.get();
    }
}