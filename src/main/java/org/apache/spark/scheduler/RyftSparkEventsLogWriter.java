package org.apache.spark.scheduler;

import org.apache.spark.SparkContext;
import org.apache.spark.deploy.history.RollingEventLogFilesWriter;
import org.apache.spark.util.JsonProtocol;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.net.URI;

public class RyftSparkEventsLogWriter implements SparkListenerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(RyftSparkEventsLogWriter.class);
    private RollingEventLogFilesWriter eventLogWriter;
    private final String eventLogDir;

    public RyftSparkEventsLogWriter(SparkContext sparkContext) {
        var sparkConf = sparkContext.getConf();

        this.eventLogDir = sparkConf.getOption("spark.eventLog.ryft.dir").getOrElse(() -> {
            LOG.error("Event log directory is not set. Can't start the Ryft event log writer");
            return null;
        });

        if (this.eventLogDir == null) return;

        LOG.info("Ryft event log directory is set to: {}", this.eventLogDir);

        sparkConf.getOption("spark.eventLog.ryft.rolling.maxFilesToRetain")
                .fold(() -> {
                    LOG.warn("Ryft event log's max files to retain is not set. using default behavior of no retention limit");
                    sparkConf.remove("spark.eventLog.rolling.maxFilesToRetain");
                    return null;
                }, maxFilesToRetainValue -> {
                    LOG.info("Ryft event log's max files to retain is set to: {}", maxFilesToRetainValue);
                    sparkConf.set("spark.eventLog.rolling.maxFilesToRetain", maxFilesToRetainValue);
                    return null;
                });

        var maxFileSize = sparkConf.getOption("spark.eventLog.ryft.rolling.maxFileSize")
                .getOrElse(() -> {
                    LOG.warn("Max file size is not set. Using default value: 10M");
                    return "10M";
                });

        var overwrite = sparkConf.getOption("spark.eventLog.ryft.rolling.overwrite")
                .getOrElse(() -> {
                    LOG.warn("Overwrite is not set. Using default value: true");
                    return "true";
                });

        var minFileWriteInterval = sparkConf.getOption("spark.eventLog.ryft.rotation.interval")
                .getOrElse(() -> {
                    LOG.warn("Min file write interval is not set. Using default value: 300s");
                    return "300s";
                });

        sparkConf.set("spark.eventLog.rolling.maxFileSize", maxFileSize);
        sparkConf.set("spark.eventLog.overwrite", overwrite);
        sparkConf.set("spark.eventLog.rotation.interval", minFileWriteInterval);

        this.eventLogWriter = new RollingEventLogFilesWriter(
                sparkContext.applicationId(),
                sparkContext.applicationAttemptId(),
                URI.create(this.eventLogDir),
                sparkConf,
                sparkContext.hadoopConfiguration()
        );`

        LOG.info("Starting ryft event log writer");
        this.eventLogWriter.start();
    }

    private void writeEventToLog(SparkListenerEvent event) {
        try {
            LOG.info("Writing to event log: {}, to destination: {}", event.getClass(), this.eventLogDir);
            String eventJson = JsonProtocol.sparkEventToJsonString(event);
            eventLogWriter.writeEvent(eventJson, true);
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
        return;
    }

    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
        return;
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        return;
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
        return;
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
        this.writeEventToLog(executorMetricsUpdate);
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
    public void onExecutorBlacklistedForStage(SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
        this.writeEventToLog(executorBlacklistedForStage);
    }

    @Override
    public void onExecutorExcludedForStage(SparkListenerExecutorExcludedForStage executorExcludedForStage) {
        this.writeEventToLog(executorExcludedForStage);
    }

    @Override
    public void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
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
    public void onUnschedulableTaskSetAdded(SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
        this.writeEventToLog(unschedulableTaskSetAdded);
    }

    @Override
    public void onUnschedulableTaskSetRemoved(SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved) {
        this.writeEventToLog(unschedulableTaskSetRemoved);
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        this.writeEventToLog(blockUpdated);
    }

    @Override
    public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
        return;
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        LOG.info("Other event of type: {}", event);
        this.writeEventToLog(event);
    }

    @Override
    public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
        this.writeEventToLog(event);
    }
}
