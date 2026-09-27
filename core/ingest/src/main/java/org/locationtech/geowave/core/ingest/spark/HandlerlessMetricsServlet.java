/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.spark;

import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.sink.MetricsServlet;
import org.sparkproject.jetty.servlet.ServletContextHandler;
import com.codahale.metrics.MetricRegistry;

/**
 * Spark's metrics servlet sink, without the servlet.
 *
 * <p> A SparkContext builds the driver's metrics servlet whether or not the UI is on, and Spark 4.0
 * and 4.1 build it on their shaded Jetty 11, which needs Servlet 5's SingleThreadModel. Servlet 6
 * removed that class, and GeoWave's classpath carries Servlet 6.1 for the Jetty 12 that GeoServer 3
 * runs on, so the default sink fails and takes the SparkContext with it. Spark 4.2 moves to Jetty
 * 12, and then this can go.
 */
public class HandlerlessMetricsServlet extends MetricsServlet {
  // Executors never build the handlers, so they keep Spark's own sink, which is also the only one a
  // cluster's executors can load before GeoWave's jar is on their classpath
  public static final String DRIVER_SINK_PROPERTY = "spark.metrics.conf.driver.sink.servlet.class";

  public HandlerlessMetricsServlet(final Properties property, final MetricRegistry registry) {
    super(property, registry);
  }

  @Override
  public ServletContextHandler[] getHandlers(final SparkConf conf) {
    return new ServletContextHandler[0];
  }
}
