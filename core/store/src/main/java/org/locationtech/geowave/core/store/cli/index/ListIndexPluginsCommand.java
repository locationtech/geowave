/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.index;

import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeRegistry;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listplugins", parentOperation = IndexSection.class)
@Parameters(commandDescription = "List supported index types")
public class ListIndexPluginsCommand extends ServiceEnabledCommand<String> {

  @Override
  public void execute(final OperationParams params) {
    JCommander.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) {
    final StringBuilder builder = new StringBuilder();

    builder.append("Available index types currently registered as plugins:\n");
    for (final Entry<String, DimensionalityTypeProviderSpi> pluginProviderEntry : DimensionalityTypeRegistry.getRegisteredDimensionalityTypes().entrySet()) {
      final DimensionalityTypeProviderSpi pluginProvider = pluginProviderEntry.getValue();
      final String desc =
          pluginProvider.getDimensionalityTypeDescription() == null ? "no description"
              : pluginProvider.getDimensionalityTypeDescription();

      builder.append(String.format("%n  %s:%n    %s%n", pluginProviderEntry.getKey(), desc));
    }

    return builder.toString();
  }
}
