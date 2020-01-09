/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.bigtable.cli;

import java.util.concurrent.TimeUnit;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "run", parentOperation = BigtableSection.class)
@Parameters(
    commandDescription = "Runs a standalone Bigtable server for test and debug with GeoWave")
public class RunBigtableEmulator extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunBigtableEmulator.class);

  @ParametersDelegate
  private RunBigtableEmulatorOptions options = new RunBigtableEmulatorOptions();
  @Parameter(
      names = {"--interactive", "-i"},
      description = "Whether to prompt for user input to end the process")
  private boolean interactive = true;

  /**
   * Prep the driver & run the operation.
   */
  @Override
  public void execute(final OperationParams params) {
    try {
      final BigtableEmulator server = options.getServer();
      server.start(options.getPort());

      if (interactive) {
        System.out.println("hit any key to shutdown ..");
        System.in.read();
        System.out.println("Shutting down!");
        server.stop();
      } else {
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              server.stop();
            } catch (final Exception e) {
              LOGGER.warn("Unable to shutdown Bigtable", e);
              System.out.println("Error shutting down Bigtable.");
            }
            System.out.println("Shutting down!");
          }
        });

        while (true) {
          Thread.sleep(TimeUnit.MILLISECONDS.convert(Long.MAX_VALUE, TimeUnit.DAYS));
        }
      }

    } catch (final Exception e) {
      LOGGER.error("Unable to run embedded Bigtable server", e);
    }

  }
}
