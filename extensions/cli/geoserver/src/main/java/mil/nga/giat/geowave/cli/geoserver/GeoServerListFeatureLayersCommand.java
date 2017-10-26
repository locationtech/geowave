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

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "listfl", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer feature layers")
public class GeoServerListFeatureLayersCommand extends
		ServiceEnabledCommand<String>
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "Workspace Name")
	private final String workspace = null;

	@Parameter(names = {
		"-ds",
		"--datastore"
	}, required = false, description = "Datastore Name")
	private final String datastore = null;

	@Parameter(names = {
		"-g",
		"--geowaveOnly"
	}, required = false, description = "Show only GeoWave feature layers (default: false)")
	private final Boolean geowaveOnly = false;

	@Override
	public boolean prepare(
			final OperationParams params ) {
		if (geoserverClient == null) {
			// Get the local config for GeoServer
			final File propFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			final GeoServerConfig config = new GeoServerConfig(
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
			final OperationParams params )
			throws Exception {
		JCommander.getConsole().println(
				computeResults(params));
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws Exception {
		final Response listLayersResponse = geoserverClient.getFeatureLayers(
				workspace,
				datastore,
				geowaveOnly);

		if (listLayersResponse.getStatus() == Status.OK.getStatusCode()) {
			final JSONObject listObj = JSONObject.fromObject(listLayersResponse.getEntity());
			return "\nGeoServer layer list: " + listObj.toString(2);
		}
		return "Error getting GeoServer layer list; code = " + listLayersResponse.getStatus();
	}
}
