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
import net.sf.json.JSONObject;

@GeowaveOperation(name = "get", parentOperation = DatastoreSection.class)
@Parameters(commandDescription = "Get GeoServer DataStore info")
public class GeoServerGetDatastoreCommand extends GeoServerCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace = null;

  @Parameter(description = "<datastore name>")
  private List<String> parameters = new ArrayList<>();

  private String datastore = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <datastore name>");
    }

    datastore = parameters.get(0);

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    final Response getStoreResponse = geoserverClient.getDatastore(workspace, datastore, false);

    if (getStoreResponse.getStatus() == Status.OK.getStatusCode()) {
      final JSONObject jsonResponse = JSONObject.fromObject(getStoreResponse.getEntity());
      final JSONObject datastore = jsonResponse.getJSONObject("dataStore");
      return "\nGeoServer store info for '" + datastore + "': " + datastore.toString(2);
    }
    final String errorMessage =
        "Error getting GeoServer store info for '"
            + datastore
            + "': "
            + getStoreResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + getStoreResponse.getStatus();
    return handleError(getStoreResponse, errorMessage);
  }
}
