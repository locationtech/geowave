/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import net.sf.json.JSONObject;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "rmfl", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer feature Layer")
public class GeoServerRemoveFeatureLayerCommand extends GeoServerRemoveCommand<String> {
  @Parameter(description = "<layer name>")
  private List<String> parameters = new ArrayList<String>();

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

    layerName = parameters.get(0);

    final Response deleteLayerResponse = geoserverClient.deleteFeatureLayer(layerName);

    if (deleteLayerResponse.getStatus() == Status.OK.getStatusCode()) {
      final JSONObject listObj = JSONObject.fromObject(deleteLayerResponse.getEntity());
      return "\nGeoServer delete layer response " + layerName + ": " + listObj.toString(2);
    }
    String errorMessage =
        "Error deleting GeoServer layer '"
            + layerName
            + "': "
            + deleteLayerResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + deleteLayerResponse.getStatus();
    return handleError(deleteLayerResponse, errorMessage);
  }
}
