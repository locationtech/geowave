/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.operations;

import org.locationtech.geowave.core.cli.spi.CLIOperationProviderSpi;

public class OSMOperationProvider implements CLIOperationProviderSpi {

  private static final Class<?>[] OPERATIONS =
      new Class<?>[] {
          OSMSection.class,
          StageOSMToHDFSCommand.class,
          IngestOSMToGeoWaveCommand.class};

  @Override
  public Class<?>[] getOperations() {
    return OPERATIONS;
  }
}
