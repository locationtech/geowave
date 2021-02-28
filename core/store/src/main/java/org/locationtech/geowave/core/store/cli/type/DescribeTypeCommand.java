/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.type;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.utils.ConsoleTablePrinter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.StoreLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "describe", parentOperation = TypeSection.class)
@Parameters(commandDescription = "Describes a type with a given name in a data store")
public class DescribeTypeCommand extends ServiceEnabledCommand<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DescribeTypeCommand.class);

  @Parameter(description = "<store name> <datatype name>")
  private List<String> parameters = new ArrayList<>();

  private DataStorePluginOptions inputStoreOptions = null;

  /** Return "200 OK" for all describe commands. */
  @Override
  public Boolean successStatusIs200() {
    return true;
  }

  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName, final String adapterId) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
    parameters.add(adapterId);
  }

  public DataStorePluginOptions getInputStoreOptions() {
    return inputStoreOptions;
  }

  @Override
  public Void computeResults(final OperationParams params) {
    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException("Requires arguments: <store name> <type name>");
    }

    final String inputStoreName = parameters.get(0);
    final String typeName = parameters.get(1);

    // Attempt to load store.
    final File configFile = getGeoWaveConfigFile(params);

    final StoreLoader inputStoreLoader = new StoreLoader(inputStoreName);
    if (!inputStoreLoader.loadFromConfig(configFile, params.getConsole())) {
      throw new ParameterException("Cannot find store name: " + inputStoreLoader.getStoreName());
    }
    inputStoreOptions = inputStoreLoader.getDataStorePlugin();

    LOGGER.info(
        "Describing everything in store: " + inputStoreName + " with type name: " + typeName);
    final PersistentAdapterStore adapterStore = inputStoreOptions.createAdapterStore();
    final InternalAdapterStore internalAdapterStore =
        inputStoreOptions.createInternalAdapterStore();
    final DataTypeAdapter<?> type =
        adapterStore.getAdapter(internalAdapterStore.getAdapterId(typeName)).getAdapter();
    final Map<String, String> description = type.describe();
    final List<List<Object>> rows = new ArrayList<>();
    for (final Entry<String, String> entry : description.entrySet()) {
      final List<Object> row = new ArrayList<>();
      row.add(entry.getKey());
      row.add(entry.getValue());
      rows.add(row);
    }
    final List<String> headers = new ArrayList<>();
    headers.add("Property");
    headers.add("Value");
    final ConsoleTablePrinter cp = new ConsoleTablePrinter(params.getConsole());
    cp.print(headers, rows);
    return null;
  }
}
