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
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rm", parentOperation = DatastoreSection.class)
@Parameters(commandDescription = "Remove GeoServer DataStore")
public class GeoServerRemoveDatastoreCommand extends GeoServerRemoveCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "Workspace Name")
  private String workspace;

  @Parameter(description = "<datastore name>")
  private List<String> parameters = new ArrayList<>();

  private String datastoreName = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <datastore name>");
    }

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    datastoreName = parameters.get(0);

    final Response deleteStoreResponse = geoserverClient.deleteDatastore(workspace, datastoreName);

    if (deleteStoreResponse.getStatus() == Status.OK.getStatusCode()) {
      return "Delete store '"
          + datastoreName
          + "' from workspace '"
          + workspace
          + "' on GeoServer: OK";
    }
    final String errorMessage =
        "Error deleting store '"
            + datastoreName
            + "' from workspace '"
            + workspace
            + "' on GeoServer: "
            + deleteStoreResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + deleteStoreResponse.getStatus();
    return handleError(deleteStoreResponse, errorMessage);
  }
}
