/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.grpc.cli;

import java.io.IOException;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "start", parentOperation = GrpcSection.class)
@Parameters(commandDescription = "Runs a gRPC service for GeoWave commands")
public class StartGrpcServerCommand extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartGrpcServerCommand.class);

  @ParametersDelegate
  private StartGrpcServerCommandOptions options = new StartGrpcServerCommandOptions();

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {

    LOGGER.info("Starting GeoWave grpc server on port: " + options.getPort());
    GeoWaveGrpcServer server = null;

    server = GeoWaveGrpcServer.getInstance();

    try {
      server.start(options.getPort());
    } catch (final IOException | NullPointerException e) {
      LOGGER.error("Exception encountered starting gRPC server", e);
    }

    if (!options.isNonBlocking()) {
      try {
        server.blockUntilShutdown();
      } catch (final InterruptedException e) {
        LOGGER.error("Exception encountered during gRPC server blockUntilShutdown()", e);
      }
    }

    LOGGER.info("GeoWave grpc server started successfully");
  }

  public void setCommandOptions(final StartGrpcServerCommandOptions opts) {
    options = opts;
  }
}
