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
import org.locationtech.geowave.cli.geoserver.GeoServerRemoveCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "rm", parentOperation = CoverageStoreSection.class)
@Parameters(commandDescription = "Remove GeoServer Coverage Store")
public class GeoServerRemoveCoverageStoreCommand extends GeoServerRemoveCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "Workspace Name")
  private String workspace;

  @Parameter(description = "<coverage store name>")
  private List<String> parameters = new ArrayList<>();

  private String cvgstoreName = null;

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
      throw new ParameterException("Requires argument: <coverage store name>");
    }

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    cvgstoreName = parameters.get(0);

    final Response deleteCvgStoreResponse =
        geoserverClient.deleteCoverageStore(workspace, cvgstoreName);

    if (deleteCvgStoreResponse.getStatus() == Status.OK.getStatusCode()) {
      return "Delete store '"
          + cvgstoreName
          + "' from workspace '"
          + workspace
          + "' on GeoServer: OK";
    }
    final String errorMessage =
        "Error deleting store '"
            + cvgstoreName
            + "' from workspace '"
            + workspace
            + "' on GeoServer: "
            + deleteCvgStoreResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + deleteCvgStoreResponse.getStatus();
    return handleError(deleteCvgStoreResponse, errorMessage);
  }
}
