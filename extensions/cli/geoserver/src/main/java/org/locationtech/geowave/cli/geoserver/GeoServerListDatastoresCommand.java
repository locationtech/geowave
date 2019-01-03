/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "listds", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer datastores")
public class GeoServerListDatastoresCommand extends GeoServerCommand<String> {
  @Parameter(
      names = {"-ws", "--workspace"},
      required = false,
      description = "workspace name")
  private String workspace;

  @Override
  public void execute(final OperationParams params) throws Exception {
    JCommander.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    final Response listStoresResponse = geoserverClient.getDatastores(workspace);

    if (listStoresResponse.getStatus() == Status.OK.getStatusCode()) {
      final JSONObject jsonResponse = JSONObject.fromObject(listStoresResponse.getEntity());
      final JSONArray datastores = jsonResponse.getJSONArray("dataStores");
      return "\nGeoServer stores list for '" + workspace + "': " + datastores.toString(2);
    }
    String errorMessage =
        "Error getting GeoServer stores list for '"
            + workspace
            + "': "
            + listStoresResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + listStoresResponse.getStatus();
    return handleError(listStoresResponse, errorMessage);
  }
}
