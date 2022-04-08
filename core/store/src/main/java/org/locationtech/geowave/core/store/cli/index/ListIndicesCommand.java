/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.index;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "list", parentOperation = IndexSection.class)
@Parameters(commandDescription = "Display all indices in a data store")
public class ListIndicesCommand extends ServiceEnabledCommand<String> {
  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Override
  public void execute(final OperationParams params) throws TargetNotFoundException {
    params.getConsole().println(computeResults(params));
  }

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String computeResults(final OperationParams params) throws TargetNotFoundException {
    if (parameters.size() < 1) {
      throw new ParameterException("Must specify store name");
    }

    final String inputStoreName = parameters.get(0);

    // Attempt to load store.
    final DataStorePluginOptions storeOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    final StringBuffer buffer = new StringBuffer();
    try (final CloseableIterator<Index> it = storeOptions.createIndexStore().getIndices()) {
      while (it.hasNext()) {
        final Index index = it.next();
        buffer.append(index.getName()).append(' ');
      }
    }
    return "Available indexes: " + buffer.toString();
  }
}
