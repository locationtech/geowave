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
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameters;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "list", parentOperation = WorkspaceSection.class)
@Parameters(commandDescription = "List GeoServer workspaces")
public class GeoServerListWorkspacesCommand extends GeoServerCommand<List<String>> {

  @Override
  public void execute(final OperationParams params) throws Exception {
    for (final String string : computeResults(params)) {
      params.getConsole().println(string);
    }
  }

  @Override
  public List<String> computeResults(final OperationParams params) throws Exception {
    final Response getWorkspacesResponse = geoserverClient.getWorkspaces();

    final ArrayList<String> results = new ArrayList<>();
    if (getWorkspacesResponse.getStatus() == Status.OK.getStatusCode()) {
      results.add("\nList of GeoServer workspaces:");

      final JSONObject jsonResponse = JSONObject.fromObject(getWorkspacesResponse.getEntity());

      final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");
      for (int i = 0; i < workspaces.size(); i++) {
        final String wsName = workspaces.getJSONObject(i).getString("name");
        results.add("  > " + wsName);
      }

      results.add("---\n");
      return results;
    }
    final String errorMessage =
        "Error getting GeoServer workspace list: "
            + getWorkspacesResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + getWorkspacesResponse.getStatus();
    return handleError(getWorkspacesResponse, errorMessage);
  }
}
