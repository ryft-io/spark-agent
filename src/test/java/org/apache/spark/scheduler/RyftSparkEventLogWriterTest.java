package org.apache.spark.scheduler;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Test;

import java.time.Duration;

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

        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval1).isDefined();
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval1).forall(x -> x.toMillis() == 300000L);
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval2).isDefined();
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval2).forall(x -> x.toMillis() == 300000L);
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval3).isDefined();
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval3).forall(x -> x.toMillis() == 18000000L);
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval5).isEmpty();
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval6).isEmpty();
        assert RyftSparkEventLogWriter.parseIntervalToDuration(interval7).isEmpty();
    }
}
