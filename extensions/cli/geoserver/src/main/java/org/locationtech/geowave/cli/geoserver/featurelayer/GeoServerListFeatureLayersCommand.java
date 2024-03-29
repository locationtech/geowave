/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.featurelayer;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "list", parentOperation = FeatureLayerSection.class)
@Parameters(commandDescription = "List GeoServer feature layers")
public class GeoServerListFeatureLayersCommand extends GeoServerCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "Workspace Name")
  private String workspace = null;

  public void setWorkspace(final String workspace) {
    this.workspace = workspace;
  }

  @Parameter(names = {"-ds", "--datastore"}, required = false, description = "Datastore Name")
  private String datastore = null;

  @Parameter(
      names = {"-g", "--geowaveOnly"},
      required = false,
      description = "Show only GeoWave feature layers (default: false)")
  private Boolean geowaveOnly = false;

  public void setGeowaveOnly(final Boolean geowaveOnly) {
    this.geowaveOnly = geowaveOnly;
  }

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  public void setDatastore(final String datastore) {
    this.datastore = datastore;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    final Response listLayersResponse =
        geoserverClient.getFeatureLayers(workspace, datastore, geowaveOnly);

    if (listLayersResponse.getStatus() == Status.OK.getStatusCode()) {
      final JSONObject listObj = JSONObject.fromObject(listLayersResponse.getEntity());
      return "\nGeoServer layer list: " + listObj.toString(2);
    }
    final String errorMessage =
        "Error getting GeoServer layer list: "
            + listLayersResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + listLayersResponse.getStatus();
    return handleError(listLayersResponse, errorMessage);
  }
}
