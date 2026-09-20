/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.cvstore;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.cli.geoserver.GeoServerJson;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.JsonNode;

@GeowaveOperation(name = "get", parentOperation = CoverageStoreSection.class)
@Parameters(commandDescription = "Get GeoServer CoverageStore info")
public class GeoServerGetCoverageStoreCommand extends GeoServerCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace;

  @Parameter(description = "<coverage store name>")
  private List<String> parameters = new ArrayList<>();

  private String csName = null;

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
      throw new ParameterException("Requires argument: <coverage store name>");
    }

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    csName = parameters.get(0);

    final Response getCvgStoreResponse = geoserverClient.getCoverageStore(workspace, csName, false);

    if (getCvgStoreResponse.getStatus() == Status.OK.getStatusCode()) {
      final JsonNode jsonResponse = GeoServerJson.parse(getCvgStoreResponse.getEntity());
      final JsonNode cvgstore = jsonResponse.get("coverageStore");
      return "\nGeoServer coverage store info for '"
          + csName
          + "': "
          + GeoServerJson.pretty(cvgstore);
    }
    final String errorMessage =
        "Error getting GeoServer coverage store info for '"
            + csName
            + "': "
            + getCvgStoreResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + getCvgStoreResponse.getStatus();
    return handleError(getCvgStoreResponse, errorMessage);
  }
}
