/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.cvstore;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "list", parentOperation = CoverageStoreSection.class)
@Parameters(commandDescription = "List GeoServer coverage stores")
public class GeoServerListCoverageStoresCommand extends GeoServerCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace;

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    final Response listCvgStoresResponse = geoserverClient.getCoverageStores(workspace);

    if (listCvgStoresResponse.getStatus() == Status.OK.getStatusCode()) {
      final JSONObject jsonResponse = JSONObject.fromObject(listCvgStoresResponse.getEntity());
      final JSONArray cvgStores = jsonResponse.getJSONArray("coverageStores");
      return "\nGeoServer coverage stores list for '" + workspace + "': " + cvgStores.toString(2);
    }
    final String errorMessage =
        "Error getting GeoServer coverage stores list for '"
            + workspace
            + "': "
            + listCvgStoresResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + listCvgStoresResponse.getStatus();
    return handleError(listCvgStoresResponse, errorMessage);
  }
}
