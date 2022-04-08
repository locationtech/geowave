/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.utils.ConsoleTablePrinter;
import org.locationtech.geowave.core.cli.utils.FirstElementListComparator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "describe", parentOperation = StoreSection.class)
@Parameters(commandDescription = "List all of the configuration parameters for a given store")
public class DescribeStoreCommand extends ServiceEnabledCommand<Map<String, String>> {
  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  public List<String> getParameters() {
    return this.parameters;
  }

  public void setParameters(final String storeName) {
    this.parameters = new ArrayList<>();
    this.parameters.add(storeName);
  }


  @Override
  public void execute(OperationParams params) throws Exception {
    Map<String, String> configMap = computeResults(params);
    List<List<Object>> rows = new ArrayList<List<Object>>(configMap.size());

    Iterator<Map.Entry<String, String>> entryIter = configMap.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<String, String> entry = entryIter.next();
      List<Object> values = new ArrayList<Object>(2);
      values.add(entry.getKey());
      values.add(entry.getValue());
      rows.add(values);
    }

    Collections.sort(rows, new FirstElementListComparator());
    new ConsoleTablePrinter(params.getConsole()).print(
        Arrays.asList("Config Parameter", "Value"),
        rows);
  }

  @Override
  public Map<String, String> computeResults(OperationParams params) throws Exception {
    if (parameters.size() < 1) {
      throw new ParameterException("Must specify store name");
    }

    final File configFile = getGeoWaveConfigFile(params);
    final Properties configProps = ConfigOptions.loadProperties(configFile);
    final String configPrefix = "store." + parameters.get(0) + ".opts.";

    Map<String, String> storeMap =
        configProps.entrySet().stream().filter(
            entry -> entry.getKey().toString().startsWith(configPrefix)).collect(
                Collectors.toMap(
                    entry -> ((String) entry.getKey()).substring(configPrefix.length()),
                    entry -> (String) entry.getValue()));
    return storeMap;
  }

}
