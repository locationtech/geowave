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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "list", parentOperation = StoreSection.class)
@Parameters(commandDescription = "List non-default geowave data stores and their associated type")
public class ListStoresCommand extends ServiceEnabledCommand<Map<String, String>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ListStoresCommand.class);


  @Override
  public void execute(OperationParams params) throws Exception {
    Map<String, String> storeMap = computeResults(params);
    List<List<Object>> rows = new ArrayList<List<Object>>(storeMap.size());
    storeMap.entrySet().forEach(entry -> {
      List<Object> values = new ArrayList<Object>(2);
      String key = entry.getKey();
      values.add(key.substring(6, key.length() - ".type".length()));
      values.add(entry.getValue());
      rows.add(values);
    });

    Collections.sort(rows, new FirstElementListComparator());
    new ConsoleTablePrinter(params.getConsole()).print(Arrays.asList("Data Store", "Type"), rows);
  }

  @Override
  public Map<String, String> computeResults(OperationParams params) throws Exception {
    final File configFile = getGeoWaveConfigFile(params);

    // ConfigOptions checks/will never return null Properties
    Properties configProps = ConfigOptions.loadProperties(configFile);
    LOGGER.debug(configProps.size() + " entries in the config file");

    // The name that the user gave the store is in a property named
    // as "store." <[optional namespace.] the name the user gave > ".type"
    Map<String, String> storeMap =
        configProps.entrySet().stream().filter(
            entry -> !entry.getKey().toString().startsWith("store.default-")) // Omit defaults
            .filter(entry -> entry.getKey().toString().startsWith("store.")).filter(
                entry -> entry.getKey().toString().endsWith(".type")).collect(
                    Collectors.toMap(
                        entry -> (String) entry.getKey(),
                        entry -> (String) entry.getValue()));
    return storeMap;
  }

}
