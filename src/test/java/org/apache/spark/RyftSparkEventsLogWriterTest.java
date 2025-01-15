package org.apache.spark;

import org.apache.spark.scheduler.RyftSparkEventsLogWriter;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.junit.Test;

public class RyftSparkEventsLogWriterTest {
  @Test
  public void test() {
    SparkConf conf = new SparkConf();
    SparkContext sc = new SparkContext("local", "SparkEventsLogWriter", conf);
    RyftSparkEventsLogWriter listener = new RyftSparkEventsLogWriter(sc);
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
}
