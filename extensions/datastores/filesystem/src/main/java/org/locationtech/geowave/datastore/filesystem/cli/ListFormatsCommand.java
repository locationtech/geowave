/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.cli;

import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatterRegistry;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatterSpi;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listformats", parentOperation = FileSystemSection.class)
@Parameters(
    commandDescription = "List available formats for usage with --format option with FileSystem datastore")
public class ListFormatsCommand extends ServiceEnabledCommand<String> {

  @Override
  public void execute(final OperationParams params) {
    params.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) {
    final StringBuilder builder = new StringBuilder();

    builder.append("Available data formats currently registered as plugins:\n");
    for (final Entry<String, FileSystemDataFormatterSpi> dataFormatterEntry : FileSystemDataFormatterRegistry.getDataFormatterRegistry().entrySet()) {
      final FileSystemDataFormatterSpi pluginProvider = dataFormatterEntry.getValue();
      final String desc =
          pluginProvider.getFormatDescription() == null ? "no description"
              : pluginProvider.getFormatDescription();
      builder.append(String.format("%n  %s:%n    %s%n", dataFormatterEntry.getKey(), desc));
    }
    return builder.toString();
  }
}
