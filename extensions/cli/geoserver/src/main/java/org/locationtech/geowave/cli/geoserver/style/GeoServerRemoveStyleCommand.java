/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.style;

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

@GeowaveOperation(name = "rm", parentOperation = StyleSection.class)
@Parameters(commandDescription = "Remove GeoServer Style")
public class GeoServerRemoveStyleCommand extends GeoServerRemoveCommand<String> {
  @Parameter(description = "<style name>")
  private List<String> parameters = new ArrayList<>();

  private String styleName = null;

  public void setParameters(final List<String> parameters) {
    this.parameters = parameters;
  }

  @Override
  public void execute(final OperationParams params) throws Exception {
    params.getConsole().println(computeResults(params));
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <style name>");
    }

    styleName = parameters.get(0);

    final Response deleteStyleResponse = geoserverClient.deleteStyle(styleName);

    if (deleteStyleResponse.getStatus() == Status.OK.getStatusCode()) {
      return "Delete style '" + styleName + "' on GeoServer: OK";
    }
    final String errorMessage =
        "Error deleting style '"
            + styleName
            + "' on GeoServer: "
            + deleteStyleResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + deleteStyleResponse.getStatus();
    return handleError(deleteStyleResponse, errorMessage);
  }
}
