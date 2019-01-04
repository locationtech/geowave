/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.remote;

import org.locationtech.geowave.core.cli.spi.CLIOperationProviderSpi;
import org.locationtech.geowave.core.store.operations.remote.MergeDataCommand;
import org.locationtech.geowave.core.store.operations.remote.VersionCommand;

public class RemoteOperationProvider implements CLIOperationProviderSpi {

  private static final Class<?>[] OPERATIONS =
      new Class<?>[] {
          RemoteSection.class,
          CalculateStatCommand.class,
          ClearCommand.class,
          ListTypesCommand.class,
          ListIndicesCommand.class,
          ListStatsCommand.class,
          VersionCommand.class,
          MergeDataCommand.class,
          RecalculateStatsCommand.class,
          CombineStatsCommand.class,
          RemoveTypeCommand.class,
          RemoveStatCommand.class};

  @Override
  public Class<?>[] getOperations() {
    return OPERATIONS;
  }
}
