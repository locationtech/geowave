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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "add", parentOperation = DatastoreSection.class)
@Parameters(commandDescription = "Add a GeoServer datastore")
public class GeoServerAddDatastoreCommand extends GeoServerCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace = null;

  @Parameter(names = {"-ds", "--datastore"}, required = false, description = "datastore name")
  private String datastore = null;

  @Parameter(description = "<GeoWave store name>")
  private List<String> parameters = new ArrayList<>();

  private String gwStore = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  public void setDatastore(final String datastore) {
    this.datastore = datastore;
  }

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <datastore name>");
    }

    gwStore = parameters.get(0);

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    final Response addStoreResponse = geoserverClient.addDatastore(workspace, datastore, gwStore);

    if ((addStoreResponse.getStatus() == Status.OK.getStatusCode())
        || (addStoreResponse.getStatus() == Status.CREATED.getStatusCode())) {
      return "Add datastore for '"
          + gwStore
          + "' to workspace '"
          + workspace
          + "' on GeoServer: OK";
    }
    final String errorMessage =
        "Error adding datastore for '"
            + gwStore
            + "' to workspace '"
            + workspace
            + "' on GeoServer: "
            + addStoreResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + addStoreResponse.getStatus();
    return handleError(addStoreResponse, errorMessage);
  }
}
