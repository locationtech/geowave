/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations;

import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import org.locationtech.geowave.core.ingest.spi.IngestFormatPluginRegistry;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listplugins", parentOperation = IngestSection.class)
@Parameters(commandDescription = "List supported ingest formats")
public class ListIngestPluginsCommand extends ServiceEnabledCommand<String> {

  @Override
  public void execute(final OperationParams params) {
    JCommander.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) {
    final StringBuilder builder = new StringBuilder();

    builder.append("Available ingest formats currently registered as plugins:\n");
    for (final Entry<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderEntry : IngestFormatPluginRegistry.getPluginProviderRegistry().entrySet()) {
      final IngestFormatPluginProviderSpi<?, ?> pluginProvider = pluginProviderEntry.getValue();
      final String desc =
          pluginProvider.getIngestFormatDescription() == null ? "no description"
              : pluginProvider.getIngestFormatDescription();
      builder.append(String.format("%n  %s:%n    %s%n", pluginProviderEntry.getKey(), desc));
    }

    return builder.toString();
  }
}
