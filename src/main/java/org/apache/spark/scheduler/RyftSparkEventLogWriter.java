package org.apache.spark.scheduler;

import io.ryft.spark.utils.StringRandomizer;

import java.net.URI;
import java.time.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.history.RollingEventLogFilesWriter;
import org.apache.spark.util.JsonProtocol;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;

/**
 * A Spark listener that writes events to a Ryft-specific event log.
 *
 * <p>This class implements {@link SparkListenerInterface} to capture and process Spark events.
 * It uses a {@link RollingEventLogFilesWriter} to manage the event log, supporting features
 * such as rolling files, retention policies, and customizable configurations. Events are serialized
 * to JSON format using Spark's {@link JsonProtocol} before being logged.</p>
 *
 * <p>Key features include:</p>
 * <ul>
 *   <li>Integration with Spark configuration options for log directory and retention settings.</li>
 *   <li>Automatic retries for initialization with exponential backoff.</li>
 *   <li>Selective logging to manage logging frequency and avoid log spam.</li>
 * </ul>
 *
 * <p>Usage:</p>
 * <p>The listener can be attached to a SparkContext to begin logging events.
 * Configuration options such as log directory, maximum file size, and retention policies
 * are specified via Spark properties prefixed with <code>spark.eventLog.ryft</code>.</p>
 *
 * <p>Example Spark properties:</p>
 * <ul>
 *   <li><code>spark.eventLog.ryft.dir</code>: Base directory for Ryft event logs.</li>
 *   <li><code>spark.eventLog.ryft.rolling.maxFileSize</code>: Maximum size of each log file.</li>
 *   <li><code>spark.eventLog.ryft.rolling.overwrite</code>: Whether to overwrite existing log files.</li>
 *   <li><code>spark.eventLog.ryft.rotation.interval</code>: Interval for log rotation.</li>
 * </ul>
 *
 * <p>Note: If initialization fails after a predefined number of attempts, the writer will stop
 * retrying. This behavior is logged for troubleshooting.</p>
 *
 * <p>Thread safety: This class is not thread-safe and should be used in the context of a single-threaded
 * Spark listener execution model.</p>
 *
 * @see org.apache.spark.scheduler.SparkListenerInterface
 * @see RollingEventLogFilesWriter
 * @see JsonProtocol
 */

public class RyftSparkEventLogWriter implements SparkListenerInterface {
    // Logger properties
    private static final Logger LOG = LoggerFactory.getLogger(RyftSparkEventLogWriter.class);
    private static final int LOG_SAMPLE_RATE = 100;

    private final Random RANDOM = new Random();

    // Spark RollingEventLogFilesWriter properties
    private RollingEventLogFilesWriter eventLogWriter;
    private static final String DEFAULT_ROLLING_FILE_MAX_SIZE = "10M";
    private static final String DEFAULT_ROLLING_FILE_MIN_SIZE = "1m";
    private static final String DEFAULT_ROLLING_OVERWRITE = "true";


    // Activation retries properties
    private static final long FIVE_MINUTES_MILLISECONDS = Duration.ofMinutes(5).toMillis();
    private static final int MAX_ATTEMPTS = 3;

    // Writer spark context properties
    private SparkConf sparkConf;
    private String applicationId;
    private String appName;
    private long startTime;
    private String user;
    private Option<String> applicationAttemptId;
    private Configuration hadoopConf;

    // Event log directory properties
    private URI eventLogDir;
    private static final int EVENT_LOG_DIR_SUFFIX_LENGTH = 6;
    private long nextInitializationAttemptTimestamp;
    private int numAttempts;

    // Interval rotation properties
    private static final Duration DEFAULT_ROLLING_INTERVAL_MS = Duration.ofMinutes(5); // 5 minutes in ms
    private Duration rollingInterval = DEFAULT_ROLLING_INTERVAL_MS;
    private Instant lastRotationTimestamp = Instant.MIN;

    public RyftSparkEventLogWriter(org.apache.spark.SparkContext sparkContext) {
        try {
            sparkConf = sparkContext.getConf();
            applicationId = sparkContext.applicationId();
            appName = sparkContext.appName();
            startTime = sparkContext.startTime();
            String user = sparkContext.sparkUser();
            applicationAttemptId = sparkContext.applicationAttemptId();
            hadoopConf = sparkContext.hadoopConfiguration();

            attemptActivateWriter();
        } catch (Exception e) {
            // In the unexpected case of unavailable sparkContext at the point of accessing the
            // configuration - catch and continue
            LOG.error("Ryft event log writer failed to extract conf from spark context", e);
        }
    }

    private URI generateEventLogDirURI(String eventLogBaseDir) {
        eventLogBaseDir = eventLogBaseDir + "/" + StringRandomizer.generateUniqueString(EVENT_LOG_DIR_SUFFIX_LENGTH) + "/";
        return URI.create(eventLogBaseDir).normalize();
    }

    private void makeBaseDir(URI eventLogBaseDir) throws Exception {
        // Permissions given here are: owner can read, write, execute; group can read, write, execute;
        // others have no permissions
        var logFolderPermissions = new FsPermission((short) 0770);
        var fileSystem = Utils.getHadoopFileSystem(eventLogBaseDir, hadoopConf);
        var logFolderPath = new Path(eventLogBaseDir);
        FileSystem.mkdirs(fileSystem, logFolderPath, logFolderPermissions);
    }

    private void writeApplicationStart() {
        LOG.info("Ryft starts writing logs for application with ID: {}, and name: {}", applicationId, appName);
        try {
            var applicationStart = new SparkListenerApplicationStart(
                    appName,
                    new Some<>(applicationId),
                    startTime,
                    user,
                    applicationAttemptId,
                    Option.empty(),
                    Option.empty()
            );

            writeEventToLog(applicationStart);
        } catch (Exception e) {
            LOG.warn("Ryft failed to write application start event, continuing", e);
        }
    }


    // we support the following interval expressions as config values following the scala duration standard:
    // <size integer><time unit> - example 5s, 5m, 5h
    public static Option<Duration> parseIntervalToDuration(String interval) {
        try {
            return new Some<>(Duration.parse("PT" + interval.toUpperCase()));
        } catch (Exception e) {
            LOG.error("Failed to parse interval expression: {}.", interval);
            return Option.empty();
        }
    }

    private void attemptActivateWriter() {
        try {
            String eventLogBaseDir =
                    sparkConf
                            .getOption("spark.eventLog.ryft.dir")
                            .getOrElse(
                                    () -> {
                                        LOG.error(
                                                "Event log directory is not set. Can't start the Ryft event log writer");
                                        return null;
                                    });

            if (eventLogBaseDir == null) return;

            ensureEventLogDirExists(eventLogBaseDir);

            LOG.info("Ryft event log directory is set to: {}", eventLogDir);

            sparkConf
                    .getOption("spark.eventLog.ryft.rolling.maxFilesToRetain")
                    .fold(
                            () -> {
                                LOG.warn(
                                        "Ryft event log's max files to retain is not set. using default behavior of no retention limit");
                                sparkConf.remove("spark.eventLog.rolling.maxFilesToRetain");
                                return null;
                            },
                            maxFilesToRetainValue -> {
                                LOG.info(
                                        "Ryft event log's max files to retain is set to: {}", maxFilesToRetainValue);
                                sparkConf.set("spark.eventLog.rolling.maxFilesToRetain", maxFilesToRetainValue);
                                return null;
                            });

            var maxFileSize =
                    sparkConf
                            .getOption("spark.eventLog.ryft.rolling.maxFileSize")
                            .getOrElse(
                                    () -> {
                                        LOG.warn("Max file size is not set. Using default value: {}", DEFAULT_ROLLING_FILE_MAX_SIZE);
                                        return DEFAULT_ROLLING_FILE_MAX_SIZE;
                                    });

            var minFileSize =
                    sparkConf
                            .getOption("spark.eventLog.ryft.rolling.minFileSize")
                            .getOrElse(
                                    () -> {
                                        LOG.warn("Min file size is not set. Using default value: {}", DEFAULT_ROLLING_FILE_MIN_SIZE);
                                        return DEFAULT_ROLLING_FILE_MIN_SIZE;
                                    });

            var overwrite =
                    sparkConf
                            .getOption("spark.eventLog.ryft.rolling.overwrite")
                            .getOrElse(
                                    () -> {
                                        LOG.warn("Overwrite is not set. Using default value: {}", DEFAULT_ROLLING_OVERWRITE);
                                        return DEFAULT_ROLLING_OVERWRITE;
                                    });

            rollingInterval =
                    sparkConf
                            .getOption("spark.eventLog.ryft.rotation.interval")
                            .flatMap(RyftSparkEventLogWriter::parseIntervalToDuration)
                            .getOrElse(
                                    () -> {
                                        LOG.info(
                                                "Ryft event log's rotation interval is not set. Using default value: {}",
                                                DEFAULT_ROLLING_INTERVAL_MS);
                                        return DEFAULT_ROLLING_INTERVAL_MS;
                                    });

            sparkConf.set("spark.eventLog.rolling.maxFileSize", maxFileSize);
            sparkConf.set("spark.eventLog.rolling.minFileSize", minFileSize);
            sparkConf.set("spark.eventLog.overwrite", overwrite);

            eventLogWriter =
                    new RollingEventLogFilesWriter(
                            applicationId, applicationAttemptId, eventLogDir, sparkConf, hadoopConf);

            LOG.info("Starting ryft event log writer");
            eventLogWriter.start();
            lastRotationTimestamp = Instant.now();
            writeApplicationStart();
        } catch (Exception e) {
            numAttempts++;
            LOG.error(
                    String.format(
                            "Failed to start ryft event log writer. Attempt %d / %d", numAttempts, MAX_ATTEMPTS),
                    e);
            eventLogWriter = null;

            if (numAttempts >= MAX_ATTEMPTS) {
                LOG.error("Ryft initialization failed after {} attempts. Gave up", numAttempts);
                nextInitializationAttemptTimestamp = 0;
            } else {
                long nextAttemptTimestamp = System.currentTimeMillis() + FIVE_MINUTES_MILLISECONDS;
                LOG.info("Ryft will try to initialize again at {}", nextAttemptTimestamp);
                nextInitializationAttemptTimestamp = nextAttemptTimestamp;
            }
        }
    }

    private void ensureEventLogDirExists(String eventLogBaseDir) throws Exception {
        if (eventLogDir != null) {
            return;
        }

        eventLogDir = generateEventLogDirURI(eventLogBaseDir);

        try {
            makeBaseDir(eventLogDir);
        } catch (Exception e) {
            LOG.error("Failed to create event log directory: {}", eventLogDir, e);
            eventLogDir = null;
            throw e;
        }
    }

    private void sampleLogWarn(String message) {
        if (RANDOM.nextInt() % LOG_SAMPLE_RATE == 0) {
            LOG.warn(message);
        }
    }

    private boolean isEventLogWriterAvailable() {
        if (eventLogWriter == null) {
            sampleLogWarn("Ryft event log writer was not initialized.");
            return false;
        }

        return true;
    }

    private boolean isEventLogDirSet() {
        if (eventLogDir == null) {
            sampleLogWarn("Ryft event log folder was not set.");
            return false;
        }

        return true;
    }

    private void writeEventToLog(SparkListenerEvent event) {
        try {
            if (!isEventLogDirSet() || !isEventLogWriterAvailable()) {
                if (nextInitializationAttemptTimestamp > 0
                        && System.currentTimeMillis() > nextInitializationAttemptTimestamp) {
                    attemptActivateWriter();
                }

                return;
            }
            LOG.debug("Writing to event log: {}, to destination: {}", event.getClass(), this.eventLogDir);
            String eventJson = JsonProtocol.sparkEventToJsonString(event);
            eventLogWriter.writeEvent(eventJson, true);
            if (lastRotationTimestamp.isAfter(Instant.MIN) && Instant.now().isAfter(lastRotationTimestamp.plus(rollingInterval))) {
                eventLogWriter.rollEventLogFile();
                lastRotationTimestamp = Instant.now();
            }
        } catch (Exception e) {
            LOG.warn("Failed to write event to {}", this.eventLogDir, e);
        }
    }

    private void closeEventLogWriter() {
        if (eventLogWriter != null) {
            eventLogWriter.stop();
        }
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart event) {
        this.writeEventToLog(event);
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted event) {
        this.writeEventToLog(event);
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
    }

    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted event) {
        this.writeEventToLog(event);
    }

    @Override
    public void onJobStart(SparkListenerJobStart event) {
        this.writeEventToLog(event);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate event) {
        this.writeEventToLog(event);
    }

    @Override
    public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
        writeEventToLog(blockManagerAdded);
    }

    @Override
    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
        writeEventToLog(blockManagerRemoved);
    }

    @Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
        writeEventToLog(unpersistRDD);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd event) {
        this.writeEventToLog(event);
        closeEventLogWriter();
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
    }

    @Override
    public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics) {
        this.writeEventToLog(executorMetrics);
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        this.writeEventToLog(executorAdded);
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        this.writeEventToLog(executorRemoved);
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        this.writeEventToLog(executorBlacklisted);
    }

    @Override
    public void onExecutorExcluded(SparkListenerExecutorExcluded executorExcluded) {
        this.writeEventToLog(executorExcluded);
    }

    @Override
    public void onExecutorBlacklistedForStage(
            SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
        this.writeEventToLog(executorBlacklistedForStage);
    }

    @Override
    public void onExecutorExcludedForStage(
            SparkListenerExecutorExcludedForStage executorExcludedForStage) {
        this.writeEventToLog(executorExcludedForStage);
    }

    @Override
    public void onNodeBlacklistedForStage(
            SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
        this.writeEventToLog(nodeBlacklistedForStage);
    }

    @Override
    public void onNodeExcludedForStage(SparkListenerNodeExcludedForStage nodeExcludedForStage) {
        this.writeEventToLog(nodeExcludedForStage);
    }

    @Override
    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
        this.writeEventToLog(executorUnblacklisted);
    }

    @Override
    public void onExecutorUnexcluded(SparkListenerExecutorUnexcluded executorUnexcluded) {
        this.writeEventToLog(executorUnexcluded);
    }

    @Override
    public void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
        this.writeEventToLog(nodeBlacklisted);
    }

    @Override
    public void onNodeExcluded(SparkListenerNodeExcluded nodeExcluded) {
        this.writeEventToLog(nodeExcluded);
    }

    @Override
    public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
        this.writeEventToLog(nodeUnblacklisted);
    }

    @Override
    public void onNodeUnexcluded(SparkListenerNodeUnexcluded nodeUnexcluded) {
        this.writeEventToLog(nodeUnexcluded);
    }

    @Override
    public void onUnschedulableTaskSetAdded(
            SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
        this.writeEventToLog(unschedulableTaskSetAdded);
    }

    @Override
    public void onUnschedulableTaskSetRemoved(
            SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved) {
        this.writeEventToLog(unschedulableTaskSetRemoved);
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        this.writeEventToLog(blockUpdated);
    }

    @Override
    public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
    }


    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        this.writeEventToLog(event);
    }

    @Override
    public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
        this.writeEventToLog(event);
    }
}
