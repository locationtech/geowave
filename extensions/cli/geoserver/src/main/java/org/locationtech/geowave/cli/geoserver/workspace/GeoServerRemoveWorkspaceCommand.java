/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.workspace;

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

@GeowaveOperation(name = "rm", parentOperation = WorkspaceSection.class)
@Parameters(commandDescription = "Remove GeoServer workspace")
public class GeoServerRemoveWorkspaceCommand extends GeoServerRemoveCommand<String> {
  @Parameter(description = "<workspace name>")
  private List<String> parameters = new ArrayList<>();

  private String wsName = null;

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
      throw new ParameterException("Requires argument: <workspace name>");
    }

    wsName = parameters.get(0);

    final Response deleteWorkspaceResponse = geoserverClient.deleteWorkspace(wsName);
    if (deleteWorkspaceResponse.getStatus() == Status.OK.getStatusCode()) {
      return "Delete workspace '" + wsName + "' from GeoServer: OK";
    }
    final String errorMessage =
        "Error deleting workspace '"
            + wsName
            + "' from GeoServer: "
            + deleteWorkspaceResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + deleteWorkspaceResponse.getStatus();
    return handleError(deleteWorkspaceResponse, errorMessage);
  }
}
