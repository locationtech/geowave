/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.cli;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.datastore.accumulo.cli.AccumuloSection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "run", parentOperation = AccumuloSection.class)
@Parameters(
    commandDescription = "Runs a standalone mini Accumulo server for test and debug with GeoWave")
public class AccumuloRunServerCommand extends DefaultOperation implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloRunServerCommand.class);

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {
    try {
      AccumuloMiniCluster.main(new String[] {});
    } catch (final Exception e) {
      LOGGER.error("Unable to run Accumulo mini cluster", e);
    }
  }
}
