package org.apache.spark.scheduler;

import org.apache.spark.deploy.history.RollingEventLogFilesWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates;
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.util.JsonProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RyftSparkEventsLogWriter extends SparkListener {
    private static final Logger LOG = LoggerFactory.getLogger(RyftSparkEventsLogWriter.class);
    private RollingEventLogFilesWriter eventLogWriter;
    private String eventLogDir;
    private final List<SparkListenerEvent> eventsBuffer = new ArrayList<>();

    private void init() {
        var maybeSession = SparkSession.getActiveSession();
        var maybeCtx = maybeSession.map(SparkSession::sparkContext);

        if (maybeCtx.isEmpty()) {
            LOG.error("Can't get the active Spark context. can't write logs to Ryft event log, will buffer events until the context is available");
            return;
        }

        var ctx = maybeCtx.get();
        LOG.info("Got the active Spark context during initialization");

        var conf = ctx.getConf();

        this.eventLogDir = conf.getOption("spark.eventLog.ryft.dir").getOrElse(() -> {
            LOG.error("Event log directory is not set. Can't start the Ryft event log writer");
            return null;
        });

        if (this.eventLogDir == null) return;

        LOG.info("Ryft event log directory is set to: {}", this.eventLogDir);

        var maxFilesToRetain = conf.getOption("spark.eventLog.ryft.rolling.maxFilesToRetain")
                .getOrElse(() -> {
                    LOG.warn("Max files to retain is not set. Using default value: 10");
                    return "10";
                });

        var maxFileSize = conf.getOption("spark.eventLog.ryft.rolling.maxFileSize")
                .getOrElse(() -> {
                    LOG.warn("Max file size is not set. Using default value: 10M");
                    return "10M";
                });

        var overwrite = conf.getOption("spark.eventLog.ryft.rolling.overwrite")
                .getOrElse(() -> {
                    LOG.warn("Overwrite is not set. Using default value: true");
                    return "true";
                });

        conf.set("spark.eventLog.rolling.maxFilesToRetain", maxFilesToRetain);
        conf.set("spark.eventLog.rolling.maxFileSize", maxFileSize);
        conf.set("spark.eventLog.overwrite", overwrite);

        this.eventLogWriter = new RollingEventLogFilesWriter(
                ctx.applicationId(),
                ctx.applicationAttemptId(),
                URI.create(this.eventLogDir),
                conf,
                ctx.hadoopConfiguration()
        );

        LOG.info("Starting ryft event log writer");
        this.eventLogWriter.start();
    }

    private void handleEvent(SparkListenerEvent event) {
        if (initializeEventLogWriterIfNeeded()) {
            processBufferedEvents();
            writeEventToLog(event);
        } else {
            bufferEvent(event);
        }
    }

    private boolean initializeEventLogWriterIfNeeded() {
        if (eventLogWriter == null) {
            LOG.info("Event log writer is not initialized yet, trying to initialize it");
            init();
        }
        return eventLogWriter != null;
    }

    private void processBufferedEvents() {
        if (!eventsBuffer.isEmpty()) {
            LOG.info("Writing buffered events to the log");
            eventsBuffer.forEach(this::writeEventToLog);
            eventsBuffer.clear();
        }
    }

    private void bufferEvent(SparkListenerEvent event) {
        LOG.warn("Event log writer is not initialized, buffering the event: {}", event.getClass());
        eventsBuffer.add(event);
    }

    private void writeEventToLog(SparkListenerEvent event) {
        try {
            LOG.info("Writing to event log: {}, to destination: {}", event.getClass(), this.eventLogDir);
            String eventJson = JsonProtocol.sparkEventToJsonString(event);
            eventLogWriter.writeEvent(eventJson, true);
            // eventLogWriter.rollEventLogFile(); // this is only to check the individual files and make sure we actually see the files being created correctly
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
        this.handleEvent(event);
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted event) {
        this.handleEvent(event);
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted event) {
        this.handleEvent(event);
    }

    @Override
    public void onJobStart(SparkListenerJobStart event) {
        this.handleEvent(event);
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate event) {
        this.handleEvent(event);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd event) {
        this.handleEvent(event);
        closeEventLogWriter();
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        // we can do other things here with the events
        LOG.info("Other event of type: {}", event);
        if (event instanceof SparkListenerSQLExecutionStart)
            this.writeEventToLog(event);
        if (event instanceof SparkListenerSQLExecutionEnd)
            this.writeEventToLog(event);
        if (event instanceof StreamingQueryListener.QueryProgressEvent)
            this.writeEventToLog(event);
        if (event instanceof SparkListenerSQLAdaptiveExecutionUpdate)
            this.writeEventToLog(event);
        if (event instanceof SparkListenerDriverAccumUpdates)
            this.writeEventToLog(event);
        if (event instanceof SparkListenerSQLExecutionEnd)
            this.writeEventToLog(event);
    }
}
