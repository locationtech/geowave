/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "rmfl", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer feature Layer")
public class GeoServerRemoveFeatureLayerCommand extends
		GeoServerCommand<String>
{
	@Parameter(description = "<layer name>")
	private List<String> parameters = new ArrayList<String>();
	private String layerName = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <layer name>");
		}

		JCommander.getConsole().println(
				computeResults(params));
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws Exception {
		layerName = parameters.get(0);

		final Response deleteLayerResponse = geoserverClient.deleteFeatureLayer(layerName);

		if (deleteLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			final JSONObject listObj = JSONObject.fromObject(deleteLayerResponse.getEntity());
			return "\nGeoServer delete layer response " + layerName + ": " + listObj.toString(2);
		}
		return "Error deleting GeoServer layer " + layerName + "; code = " + deleteLayerResponse.getStatus();
	}
}
