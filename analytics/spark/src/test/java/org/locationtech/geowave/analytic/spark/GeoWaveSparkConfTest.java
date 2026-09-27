/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark;

import static org.junit.Assert.assertEquals;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Assume;
import org.junit.Test;

public class GeoWaveSparkConfTest {

  private static boolean onClasspath(final String className) {
    try {
      Class.forName(className);
      return true;
    } catch (final ClassNotFoundException e) {
      return false;
    }
  }

  @Test
  public void defaultSessionStartsWithServlet6() {
    // Servlet 6 removed SingleThreadModel, which Spark 4.0's Jetty 11 servlets need
    Assume.assumeFalse(onClasspath("jakarta.servlet.SingleThreadModel"));
    final SparkSession session =
        GeoWaveSparkConf.createDefaultSession(
            new SparkConf().setMaster("local[1]").setAppName("GeoWaveSparkConfTest").set(
                "spark.driver.host",
                "127.0.0.1").set("spark.driver.bindAddress", "127.0.0.1"));
    try {
      assertEquals(3L, session.range(3).count());
    } finally {
      session.stop();
    }
  }
}
