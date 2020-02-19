/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.layer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.geowave.cli.geoserver.GeoServerCommand;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "add", parentOperation = LayerSection.class)
@Parameters(commandDescription = "Add a GeoServer layer from the given GeoWave store")
public class GeoServerAddLayerCommand extends GeoServerCommand<String> {
  public static enum AddOption {
    ALL, RASTER, VECTOR;
  }

  @Parameter(names = {"-ws", "--workspace"}, required = false, description = "workspace name")
  private String workspace = null;

  @Parameter(
      names = {"-a", "--add"},
      converter = AddOptionConverter.class,
      description = "For multiple layers, add (all | raster | vector)")
  private AddOption addOption = null;

  @Parameter(names = {"-t", "--typeName"}, description = "The type to add to GeoServer")
  private String adapterId = null;

  @Parameter(names = {"-sld", "--setStyle"}, description = "default style sld")
  private String style = null;

  @Parameter(description = "<GeoWave store name>")
  private List<String> parameters = new ArrayList<>();

  private String gwStore = null;

  @Override
  public void execute(final OperationParams params) throws Exception {
    JCommander.getConsole().println(computeResults(params));
  }

  public static class AddOptionConverter implements IStringConverter<AddOption> {
    @Override
    public AddOption convert(final String value) {
      final AddOption convertedValue = AddOption.valueOf(value.toUpperCase());

      if ((convertedValue != AddOption.ALL)
          && (convertedValue != AddOption.RASTER)
          && (convertedValue != AddOption.VECTOR)) {
        throw new ParameterException(
            "Value "
                + value
                + "can not be converted to an add option. "
                + "Available values are: "
                + StringUtils.join(AddOption.values(), ", ").toLowerCase(Locale.ENGLISH));
      }
      return convertedValue;
    }
  }

  @Override
  public String computeResults(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <store name>");
    }

    gwStore = parameters.get(0);

    if ((workspace == null) || workspace.isEmpty()) {
      workspace = geoserverClient.getConfig().getWorkspace();
    }

    if (addOption != null) { // add all supercedes specific adapter
      // selection
      adapterId = addOption.name();
    }

    final Response addLayerResponse =
        geoserverClient.addLayer(workspace, gwStore, adapterId, style);

    if (addLayerResponse.getStatus() == Status.OK.getStatusCode()) {
      final JSONObject jsonResponse = JSONObject.fromObject(addLayerResponse.getEntity());
      return "Add GeoServer layer for '" + gwStore + ": OK : " + jsonResponse.toString(2);
    }
    final String errorMessage =
        "Error adding GeoServer layer for store '"
            + gwStore
            + "': "
            + addLayerResponse.getEntity()
            + "\nGeoServer Response Code = "
            + addLayerResponse.getStatus();
    return handleError(addLayerResponse, errorMessage);
  }
}
