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
import java.util.Arrays;
import java.util.List;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "compact", parentOperation = IndexSection.class)
@Parameters(commandDescription = "Compact all rows for a given index")
public class CompactIndexCommand extends DefaultOperation implements Command {
  @Parameter(description = "<store name> <index name>")
  private List<String> parameters = new ArrayList<>();

  private DataStorePluginOptions inputStoreOptions = null;

  private List<Index> inputIndices = null;

  /** Prep the driver & run the operation. */
  @Override
  public void execute(final OperationParams params) {

    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <index name>");
    }

    final String inputStoreName = parameters.get(0);
    final String indexList = parameters.get(1);

    // Attempt to load store.
    inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());

    // Load the Indexes
    inputIndices = DataStoreUtils.loadIndices(inputStoreOptions.createIndexStore(), indexList);

    final PersistentAdapterStore adapterStore = inputStoreOptions.createAdapterStore();
    final InternalAdapterStore internalAdapterStore =
        inputStoreOptions.createInternalAdapterStore();
    final AdapterIndexMappingStore adapterIndexMappingStore =
        inputStoreOptions.createAdapterIndexMappingStore();
    final DataStoreOperations operations = inputStoreOptions.createDataStoreOperations();

    for (final Index index : inputIndices) {
      if (!operations.mergeData(
          index,
          adapterStore,
          internalAdapterStore,
          adapterIndexMappingStore,
          inputStoreOptions.getFactoryOptions().getStoreOptions().getMaxRangeDecomposition())) {
        params.getConsole().println("Unable to merge data within index '" + index.getName() + "'");
      } else {
        params.getConsole().println(
            "Data successfully merged within index '" + index.getName() + "'");
      }
    }
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName, final String adapterId) {
    parameters = Arrays.asList(storeName, adapterId);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }
}
