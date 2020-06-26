/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import org.locationtech.geowave.core.cli.spi.CLIOperationProviderSpi;

public class Landsat8OperationProvider implements CLIOperationProviderSpi {
  private static final Class<?>[] OPERATIONS =
      new Class<?>[] {
          Landsat8Section.class,
          Landsat8AnalyzeCommand.class,
          Landsat8DownloadCommand.class,
          Landsat8IngestCommand.class,
          Landsat8IngestRasterCommand.class,
          Landsat8IngestVectorCommand.class,};

  @Override
  public Class<?>[] getOperations() {
    return OPERATIONS;
  }
}