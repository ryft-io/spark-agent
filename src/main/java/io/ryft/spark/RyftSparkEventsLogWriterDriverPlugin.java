package io.ryft.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.scheduler.RyftSparkEventsLogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * A Spark driver plugin for integrating Ryft Spark event logging.
 * <p>
 * This plugin initializes Ryft Spark event logging in the driver context by adding
 * {@link RyftSparkEventsLogWriter} as a Spark listener.
 * </p>
 */
public class RyftSparkEventsLogWriterDriverPlugin implements DriverPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(RyftSparkEventsLogWriterDriverPlugin.class);
    private SparkContext sc;

    /**
     * Initializes the driver plugin with the provided Spark context and plugin context.
     *
     * @param sc            the Spark context for the driver
     * @param pluginContext the plugin context for the driver plugin
     * @return an empty map, as no additional configuration is needed
     */
    @Override
    public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
        this.sc = sc;
        return Collections.emptyMap();
    }

    /**
     * Registers Ryft Spark event logging metrics in the driver.
     * <p>
     * Adds {@link RyftSparkEventsLogWriter} as a Spark listener to start logging events.
     * </p>
     *
     * @param appId         the application ID
     * @param pluginContext the plugin context for the driver plugin
     */
    @Override
    public void registerMetrics(String appId, PluginContext pluginContext) {
        LOG.info("RyftSparkEventsLogWriter class loaded: {}", RyftSparkEventsLogWriter.class.getName());
        sc.addSparkListener(new RyftSparkEventsLogWriter(sc));
    }
}
