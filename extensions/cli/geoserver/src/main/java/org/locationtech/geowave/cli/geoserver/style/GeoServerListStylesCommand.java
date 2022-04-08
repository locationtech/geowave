/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.style;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameters;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "list", parentOperation = StyleSection.class)
@Parameters(commandDescription = "List GeoServer styles")
public class GeoServerListStylesCommand extends GeoServerCommand<String> {

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    final Response listStylesResponse = geoserverClient.getStyles();

    if (listStylesResponse.getStatus() == Status.OK.getStatusCode()) {
      final JSONObject jsonResponse = JSONObject.fromObject(listStylesResponse.getEntity());
      final JSONArray styles = jsonResponse.getJSONArray("styles");
      return "\nGeoServer styles list: " + styles.toString(2);
    }
    final String errorMessage =
        "Error getting GeoServer styles list: "
            + listStylesResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + listStylesResponse.getStatus();
    return handleError(listStylesResponse, errorMessage);
  }
}
