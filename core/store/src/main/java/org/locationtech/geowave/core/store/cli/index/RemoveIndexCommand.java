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
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rm", parentOperation = IndexSection.class)
@Parameters(commandDescription = "Remove an index and all associated data from a data store")
public class RemoveIndexCommand extends ServiceEnabledCommand<String> {

  @Parameter(description = "<store name> <index name>")
  private List<String> parameters = new ArrayList<>();

  @Override
  public void execute(final OperationParams params) throws Exception {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <index name>");
    }

    computeResults(params);
  }

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    final String storeName = parameters.get(0);
    final String indexName = parameters.get(1);

    // Attempt to load store.
    final DataStorePluginOptions storeOptions =
        CLIUtils.loadStore(storeName, getGeoWaveConfigFile(params), params.getConsole());

    final IndexStore indexStore = storeOptions.createIndexStore();

    final Index existingIndex = indexStore.getIndex(indexName);
    if (existingIndex == null) {
      throw new TargetNotFoundException(indexName + " does not exist");
    }

    indexStore.removeIndex(indexName);

    return "index." + indexName + " successfully removed";
  }

  @Override
  public Boolean successStatusIs200() {
    return true;
  }
}
