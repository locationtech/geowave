/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.python;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

public class GeoWavePy4JGateway {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWavePy4JGateway.class);

  /**
   * Declaring public fields which act as "submodules"
   */

  private final Debug debug = new Debug();

  public Debug getDebug() {
    return debug;
  }

  public static void main(final String[] args) throws InterruptedException {
    final GatewayServer server = new GatewayServer(new GeoWavePy4JGateway());
    GatewayServer.turnLoggingOn();

    server.start();

    System.out.println("GeoWave Py4J Gateway started...");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down GeoWave Py4J Gateway!");
        try {
          server.shutdown();
        } catch (final Exception e) {
          LOGGER.warn("Error shutting down Py4J Gateway", e);
          System.out.println("Error shutting down Py4J Gateway.");
        }
      }
    });

    while (true) {
      Thread.sleep(TimeUnit.MILLISECONDS.convert(Long.MAX_VALUE, TimeUnit.DAYS));
    }
  }

}
