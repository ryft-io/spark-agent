package org.apache.spark.scheduler;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Test;

public class RyftSparkEventLogWriterTest {
  @Test
  public void test() {
    SparkConf conf = new SparkConf();
    SparkContext sc = new SparkContext("local", "SparkEventLogWriter", conf);
    RyftSparkEventLogWriter listener = new RyftSparkEventLogWriter(sc);
    sc.addSparkListener(listener);

    sc.listenerBus()
        .post(
            new SparkListenerEvent() {
              @Override
              public boolean logEvent() {
                return SparkListenerEvent.super.logEvent();
              }
            });
  }


    @Test
    public void testIntervalExpressionSupport() {
      // valid interval expressions
        var interval1 = "300s";
        var interval2 = "5m";
        var interval3 = "5h";
        // invalid interval expressions
        var interval5 = "5";
        var interval6 = "5 milliseconds";
        var interval7 = "5 Seconds";

        assert RyftSparkEventLogWriter.parseIntervalToMs(interval1, 0L) == 300000L;
        assert RyftSparkEventLogWriter.parseIntervalToMs(interval2, 0L) == 300000L;
        assert RyftSparkEventLogWriter.parseIntervalToMs(interval3, 0L) == 18000000L;
        assert RyftSparkEventLogWriter.parseIntervalToMs(interval5, 0L) == 0L;
        assert RyftSparkEventLogWriter.parseIntervalToMs(interval6, 0L) == 0L;
        assert RyftSparkEventLogWriter.parseIntervalToMs(interval7, 0L) == 0L;
    }
}
