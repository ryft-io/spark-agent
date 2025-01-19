package io.ryft.spark;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/**
 * A Spark plugin implementation for integrating Ryft Spark event logging.
 * <p>
 * This plugin provides driver and executor plugins specifically tailored for Ryft's event logging needs.
 * </p>
 */
public class RyftSparkEventLogPlugin implements SparkPlugin {

    /**
     * Returns the driver plugin instance for Ryft Spark event logging.
     *
     * @return a {@link DriverPlugin} instance for Ryft Spark event logging
     */
    @Override
    public DriverPlugin driverPlugin() {
        return new RyftSparkEventLogWriterDriverPlugin();
    }

    /**
     * Returns the executor plugin instance for Ryft Spark event logging.
     * Currently not implemented (returns null).
     *
     * @return null, indicating that executor plugin functionality is not implemented
     */
    @Override
    public ExecutorPlugin executorPlugin() {
        return null;
    }
}
