/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.datastore;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = {"getsa", "getstoreadapters"}, parentOperation = DatastoreSection.class)
@Parameters(commandDescription = "Get GeoWave store adapters")
public class GeoServerGetStoreAdapterCommand extends GeoServerCommand<List<String>> {
  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  private String storeName = null;

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public void execute(final OperationParams params) throws Exception {
    final List<String> adapterList = computeResults(params);

    params.getConsole().println("Store " + storeName + " has these adapters:");
    for (final String adapterId : adapterList) {
      params.getConsole().println(adapterId);
    }
  }

  @Override
  public List<String> computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <store name>");
    }
    storeName = parameters.get(0);
    final List<String> adapterList = geoserverClient.getStoreAdapters(storeName, null);
    return adapterList;
  }
}
