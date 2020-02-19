/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.store;

import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listplugins", parentOperation = StoreSection.class)
@Parameters(commandDescription = "List supported data store types")
public class ListStorePluginsCommand extends ServiceEnabledCommand<String> {

  @Override
  public void execute(final OperationParams params) {
    JCommander.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) {
    final StringBuilder builder = new StringBuilder();

    builder.append("Available datastores currently registered:\n");
    final Map<String, StoreFactoryFamilySpi> dataStoreFactories =
        GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies();
    for (final Entry<String, StoreFactoryFamilySpi> dataStoreFactoryEntry : dataStoreFactories.entrySet()) {
      final StoreFactoryFamilySpi dataStoreFactory = dataStoreFactoryEntry.getValue();
      final String desc =
          dataStoreFactory.getDescription() == null ? "no description"
              : dataStoreFactory.getDescription();
      builder.append(String.format("%n  %s:%n    %s%n", dataStoreFactory.getType(), desc));
    }

    return builder.toString();
  }
}
