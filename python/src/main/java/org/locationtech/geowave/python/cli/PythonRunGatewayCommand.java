/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.python.cli;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.python.GeoWavePy4JGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rungateway", parentOperation = PythonSection.class)
@Parameters(commandDescription = "Runs a Py4J java gateway")
public class PythonRunGatewayCommand extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PythonRunGatewayCommand.class);

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {
    try {
      GeoWavePy4JGateway.main(new String[] {});
    } catch (final Exception e) {
      LOGGER.error("Unable to run Py4J gateway", e);
    }
  }
}
