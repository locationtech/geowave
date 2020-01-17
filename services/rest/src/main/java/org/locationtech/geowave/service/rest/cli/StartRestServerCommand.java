/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.cli;

import java.util.concurrent.TimeUnit;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.service.rest.ApiRestletApplication;
import org.locationtech.geowave.service.rest.ApiRestletApplicationCLI;
import org.restlet.Component;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "start", parentOperation = RestSection.class)
@Parameters(commandDescription = "Runs a rest service for GeoWave commands")
public class StartRestServerCommand extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartRestServerCommand.class);

  @ParametersDelegate
  private StartRestServerCommandOptions options = new StartRestServerCommandOptions();
  @Parameter(
      names = {"--interactive", "-i"},
      description = "Whether to prompt for user input to end the process")
  private boolean interactive = true;

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {
    try {
      final Component component = new Component();
      String port = options.getPort();
      component.getServers().add(Protocol.HTTP, Integer.parseInt(port));
      component.getDefaultHost().attach("", new ApiRestletApplicationCLI(port));
      component.start();

      LOGGER.info("GeoWave rest server started successfully");

      if (interactive) {
        System.out.println("hit any key to shutdown ..");
        System.in.read();
        System.out.println("Shutting down!");
        component.stop();
      } else {
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              component.stop();
            } catch (final Exception e) {
              LOGGER.warn("Unable to shutdown rest server", e);
              System.out.println("Error shutting down rest server.");
            }
            System.out.println("Shutting down!");
          }
        });

        while (true) {
          Thread.sleep(TimeUnit.MILLISECONDS.convert(Long.MAX_VALUE, TimeUnit.DAYS));
        }
      }
    } catch (Exception e) {
      LOGGER.error("Exception encountered starting rest server", e);
    }

  }

  public void setCommandOptions(final StartRestServerCommandOptions opts) {
    options = opts;
  }
}
