/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.store;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.server.ServerSideOperations;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

/** Command for trying to retrieve the version of GeoWave for a remote datastore */
@GeowaveOperation(name = "version", parentOperation = StoreSection.class)
@Parameters(commandDescription = "Get the version of GeoWave used by a data store")
public class VersionCommand extends ServiceEnabledCommand<String> {
  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Override
  public void execute(final OperationParams params) throws Exception {
    computeResults(params);
  }

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() < 1) {
      throw new ParameterException("Must specify store name");
    }

    final String inputStoreName = parameters.get(0);

    final DataStorePluginOptions inputStoreOptions =
        CLIUtils.loadStore(inputStoreName, getGeoWaveConfigFile(params), params.getConsole());
    // TODO: This return probably should be formatted as JSON
    final DataStoreOperations ops = inputStoreOptions.createDataStoreOperations();
    if ((ops instanceof ServerSideOperations)
        && inputStoreOptions.getFactoryOptions().getStoreOptions().isServerSideLibraryEnabled()) {
      params.getConsole().println(
          "Looking up remote datastore version for type ["
              + inputStoreOptions.getType()
              + "] and name ["
              + inputStoreName
              + "]");
      final String version = "Version: " + ((ServerSideOperations) ops).getVersion();
      params.getConsole().println(version);
      return version;
    } else {
      final String ret1 =
          "Datastore for type ["
              + inputStoreOptions.getType()
              + "] and name ["
              + inputStoreName
              + "] does not have a serverside library enabled.";
      params.getConsole().println(ret1);
      final String ret2 = "Commandline Version: " + VersionUtils.getVersion();
      params.getConsole().println(ret2);
      return ret1 + '\n' + ret2;
    }
  }

  @Override
  public HttpMethod getMethod() {
    return HttpMethod.GET;
  }
}
