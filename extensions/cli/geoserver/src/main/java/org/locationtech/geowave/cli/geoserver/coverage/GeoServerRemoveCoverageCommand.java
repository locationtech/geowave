/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.coverage;

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

@GeowaveOperation(name = "rm", parentOperation = CoverageSection.class)
@Parameters(commandDescription = "Remove a GeoServer coverage")
public class GeoServerRemoveCoverageCommand extends GeoServerRemoveCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace = null;

  @Parameter(names = {"-cs", "--cvgstore"}, required = true, description = "coverage store name")
  private String cvgstore = null;

  @Parameter(description = "<coverage name>")
  private List<String> parameters = new ArrayList<>();

  private String cvgName = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <coverage name>");
    }

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    cvgName = parameters.get(0);

    final Response getCvgResponse = geoserverClient.deleteCoverage(workspace, cvgstore, cvgName);

    if (getCvgResponse.getStatus() == Status.OK.getStatusCode()) {
      return "\nRemove GeoServer coverage '" + cvgName + "': OK";
    }
    final String errorMessage =
        "Error removing GeoServer coverage '"
            + cvgName
            + "': "
            + getCvgResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + getCvgResponse.getStatus();
    return handleError(getCvgResponse, errorMessage);
  }

  public void setCvgstore(String cvgstore) {
    this.cvgstore = cvgstore;
  }

  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }
}
