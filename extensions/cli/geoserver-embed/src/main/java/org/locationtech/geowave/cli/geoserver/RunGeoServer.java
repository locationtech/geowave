/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Server;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "run", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Runs an embedded GeoServer for test and debug with GeoWave")
public class RunGeoServer extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunGeoServer.class);

  @ParametersDelegate
  private final RunGeoServerOptions options = new RunGeoServerOptions();
  @Parameter(
      names = {"--interactive", "-i"},
      description = "Whether to prompt for user input to end the process")
  private boolean interactive = false;

  /**
   * Prep the driver & run the operation.
   */
  @Override
  public void execute(final OperationParams params) {

    Server jettyServer = null;
    try {
      jettyServer = options.getServer();

      jettyServer.start();
      if (interactive) {
        System.out.println("Press Enter to shutdown..");
        System.in.read();
        System.out.println("Shutting down!");
        jettyServer.stop();
      } else {
        final Server stopServer = jettyServer;
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              stopServer.stop();
            } catch (final Exception e) {
              LOGGER.warn("Unable to shutdown GeoServer", e);
              System.out.println("Error shutting down GeoServer.");
            }
            System.out.println("Shutting down!");
          }
        });

        while (true) {
          Thread.sleep(TimeUnit.MILLISECONDS.convert(Long.MAX_VALUE, TimeUnit.DAYS));
        }
      }
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      LOGGER.error("Could not start the Jetty server for GeoServer: " + e.getMessage(), e);
      if ((jettyServer != null) && jettyServer.isRunning()) {
        try {
          jettyServer.stop();
        } catch (final Exception e1) {
          LOGGER.error("Unable to stop the Jetty server for GeoServer", e1);
        }
      }
    }
  }
}
