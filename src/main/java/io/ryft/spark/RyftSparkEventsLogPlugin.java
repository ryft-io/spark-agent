package io.ryft.spark;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

public class RyftSparkEventsLogPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return new RyftSparkEventsLogWriterDriverPlugin();
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return null;
    }
}

