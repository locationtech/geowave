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

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "liststyles", parentOperation = GeoServerSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "List GeoServer styles")
public class GeoServerListStylesCommand extends
		DefaultOperation<String> implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

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
		JCommander.getConsole().println(
				computeResults(params));
	}

	@Override
	public String computeResults(
			OperationParams params )
			throws Exception {
		Response listStylesResponse = geoserverClient.getStyles();

		if (listStylesResponse.getStatus() == Status.OK.getStatusCode()) {
			JSONObject jsonResponse = JSONObject.fromObject(listStylesResponse.getEntity());
			JSONArray styles = jsonResponse.getJSONArray("styles");
			return "\nGeoServer styles list: " + styles.toString(2);
		}
		return "Error getting GeoServer styles list; code = " + listStylesResponse.getStatus();
	}
}
