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
}
