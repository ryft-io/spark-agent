package io.ryft.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.scheduler.RyftSparkEventsLogWriter;

import java.util.Collections;
import java.util.Map;

public class RyftSparkEventsLogWriterDriverPlugin implements DriverPlugin {
    SparkContext sc;

    @Override
    public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
        this.sc = sc;
        return Collections.emptyMap();
    }

    @Override
    public void registerMetrics(String appId, PluginContext pluginContext) {
        sc.addSparkListener(new RyftSparkEventsLogWriter(sc));
    }
}
