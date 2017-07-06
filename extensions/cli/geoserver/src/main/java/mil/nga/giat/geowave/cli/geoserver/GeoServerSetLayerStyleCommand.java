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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "setls", parentOperation = GeoServerSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "Set GeoServer Layer Style")
public class GeoServerSetLayerStyleCommand extends
		DefaultOperation<String> implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-sn",
		"--styleName"
	}, required = true, description = "<style name>")
	private String styleName = null;

	@Parameter(description = "<layer name>")
	private List<String> parameters = new ArrayList<String>();
	private String layerName = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		if (geoserverClient == null) {
			// Get the local config for GeoServer
			File propFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			GeoServerConfig config = new GeoServerConfig(
					propFile);

			// Create the rest client
			geoserverClient = new GeoServerRestClient(
					config);
		}

		// Successfully prepared
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <layer name>");
		}

	}

	@Override
	public String computeResults(
			OperationParams params )
			throws Exception {
		layerName = parameters.get(0);

		Response setLayerStyleResponse = geoserverClient.setLayerStyle(
				layerName,
				styleName);

		if (setLayerStyleResponse.getStatus() == Status.OK.getStatusCode()) {
			final String style = IOUtils.toString((InputStream) setLayerStyleResponse.getEntity());
			return "Set style for GeoServer layer '" + layerName + ": OK" + style;

		}
		return "Error setting style for GeoServer layer '" + layerName + "'; code = "
				+ setLayerStyleResponse.getStatus() + " ; " + setLayerStyleResponse.getStatusInfo().toString();
	}
}
