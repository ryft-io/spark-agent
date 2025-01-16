package org.apache.spark.scheduler;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Test;

import java.net.URI;

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

  @Test
  public void test2() {
    var eventDir = "eventDir/";

    eventDir = "s3://" + eventDir + "/" + "sdfsdf/";

    var uri = URI.create(eventDir).normalize();

    System.out.println(uri);
  }
}
