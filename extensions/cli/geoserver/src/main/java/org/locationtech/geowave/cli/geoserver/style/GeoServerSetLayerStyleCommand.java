/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.style;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.io.IOUtils;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "set", parentOperation = StyleSection.class)
@Parameters(commandDescription = "Set GeoServer Layer Style")
public class GeoServerSetLayerStyleCommand extends GeoServerCommand<String> {
  /** Return "200 OK" for the set layer command. */
  @Override
  public Boolean successStatusIs200() {
    return true;
  }

  @Parameter(names = {"-sn", "--styleName"}, required = true, description = "style name")
  private String styleName = null;

  @Parameter(description = "<layer name>")
  private List<String> parameters = new ArrayList<>();

  private String layerName = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  public void setStyleName(final String styleName) {
    this.styleName = styleName;
  }

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <layer name>");
    }

    layerName = parameters.get(0);

    final Response setLayerStyleResponse = geoserverClient.setLayerStyle(layerName, styleName);

    if (setLayerStyleResponse.getStatus() == Status.OK.getStatusCode()) {
      final String style = IOUtils.toString((InputStream) setLayerStyleResponse.getEntity());
      return "Set style for GeoServer layer '" + layerName + ": OK" + style;
    }
    final String errorMessage =
        "Error setting style for GeoServer layer '"
            + layerName
            + "': "
            + setLayerStyleResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + setLayerStyleResponse.getStatus();
    return handleError(setLayerStyleResponse, errorMessage);
  }
}
