/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.featurelayer;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "add", parentOperation = FeatureLayerSection.class)
@Parameters(commandDescription = "Add a GeoServer feature layer")
public class GeoServerAddFeatureLayerCommand extends GeoServerCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace = null;

  @Parameter(names = {"-ds", "--datastore"}, required = true, description = "datastore name")
  private String datastore = null;

  @Parameter(description = "<layer name>")
  private List<String> parameters = new ArrayList<>();

  private String layerName = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    JCommander.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <layer name>");
    }

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    layerName = parameters.get(0);

    final Response addLayerResponse =
        geoserverClient.addFeatureLayer(workspace, datastore, layerName, null);

    if (addLayerResponse.getStatus() == Status.CREATED.getStatusCode()) {
      final JSONObject listObj = JSONObject.fromObject(addLayerResponse.getEntity());
      return "\nGeoServer add layer response " + layerName + ":" + listObj.toString(2);
    }
    final String errorMessage =
        "Error adding GeoServer layer "
            + layerName
            + ": "
            + addLayerResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + addLayerResponse.getStatus();
    return handleError(addLayerResponse, errorMessage);
  }
}
