/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.cvstore;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "add", parentOperation = CoverageStoreSection.class)
@Parameters(commandDescription = "Add a GeoServer coverage store")
public class GeoServerAddCoverageStoreCommand extends GeoServerCommand<String> {
  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace = null;

  @Parameter(
      names = {"-cs", "--coverageStore"},
      required = false,
      description = "coverage store name")
  private String coverageStore = null;

  @Parameter(
      names = {"-histo", "--equalizeHistogramOverride"},
      required = false,
      description = "This parameter will override the behavior to always perform histogram equalization if a histogram exists.  Valid values are true and false.",
      arity = 1)
  private Boolean equalizeHistogramOverride = null;

  @Parameter(
      names = {"-interp", "--interpolationOverride"},
      required = false,
      description = "This will override the default interpolation stored for each layer.  Valid values are 0, 1, 2, 3 for NearestNeighbor, Bilinear, Bicubic, and Bicubic (polynomial variant) resepctively. ")
  private String interpolationOverride = null;

  @Parameter(
      names = {"-scale", "--scaleTo8Bit"},
      required = false,
      description = "By default, integer values will automatically be scaled to 8-bit and floating point values will not.  This can be overridden setting this value to true or false.",
      arity = 1)
  private Boolean scaleTo8Bit = null;

  @Parameter(description = "<GeoWave store name>")
  private List<String> parameters = new ArrayList<>();

  private String gwStore = null;

  public void setCoverageStore(final String coverageStore) {
    this.coverageStore = coverageStore;
  }

  public void setEqualizeHistogramOverride(final Boolean equalizeHistogramOverride) {
    this.equalizeHistogramOverride = equalizeHistogramOverride;
  }

  public void setInterpolationOverride(final String interpolationOverride) {
    this.interpolationOverride = interpolationOverride;
  }

  public void setScaleTo8Bit(final Boolean scaleTo8Bit) {
    this.scaleTo8Bit = scaleTo8Bit;
  }

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
      throw new ParameterException("Requires argument: <GeoWave store name>");
    }

    gwStore = parameters.get(0);

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    final Response addStoreResponse =
        geoserverClient.addCoverageStore(
            workspace,
            coverageStore,
            gwStore,
            equalizeHistogramOverride,
            interpolationOverride,
            scaleTo8Bit);

    if ((addStoreResponse.getStatus() == Status.OK.getStatusCode())
        || (addStoreResponse.getStatus() == Status.CREATED.getStatusCode())) {
      return "Add coverage store for '"
          + gwStore
          + "' to workspace '"
          + workspace
          + "' on GeoServer: OK";
    }
    final String errorMessage =
        "Error adding coverage store for '"
            + gwStore
            + "' to workspace '"
            + workspace
            + "' on GeoServer: "
            + addStoreResponse.readEntity(String.class)
            + "\nGeoServer Response Code = "
            + addStoreResponse.getStatus();
    return handleError(addStoreResponse, errorMessage);
  }
}
