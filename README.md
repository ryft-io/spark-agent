# Ryft Spark Plugin

## What is the Ryft Spark Plugin?

The Ryft Spark plugin is a pluggable Spark listener that allows you to write Spark event logs to an external destination, independent of Spark's default event logging. It provides configurable options for the log destination, log frequency, and rolling file limits.

## How to Use It?

You can integrate the Ryft Spark plugin into your Spark application in multiple ways:

### 1. Programmatically Add the Configuration

You can configure the Ryft Spark plugin directly in your Spark application code:

```scala
val spark = SparkSession.builder()
      .appName("MySparkApp")
      // Other configurations
      .config("spark.jars.packages", "io.ryft:spark:0.2.3") // Add the package
      .config("spark.plugins", "io.ryft.spark.RyftSparkEventLogPlugin") // Set the Ryft Spark plugin
      .config("spark.eventLog.ryft.dir", "s3://<your-bucket>/<ryft-spark-events-logs>/") // Specify the log destination
      .getOrCreate()
```


### 2. Add the Configuration to the Spark Configuration File `spark-defaults.conf`

Alternatively, you can add the necessary configurations to your spark-defaults.conf file:

```properties
# Add the Ryft Spark plugin package
spark.jars.packages io.ryft:spark:0.2.3
# Set the Ryft Spark plugin
spark.plugins io.ryft.spark.RyftSparkEventLogPlugin
# Specify the log destination
spark.eventLog.ryft.dir s3://<your-bucket>/<ryft-spark-events-logs>/
```