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
import java.util.ArrayList;
import java.util.List;

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

@GeowaveOperation(name = "listws", parentOperation = GeoServerSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "List GeoServer workspaces")
public class GeoServerListWorkspacesCommand extends
		DefaultOperation<List<String>> implements
		Command
{
	private GeoServerRestClient geoserverClient = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		// Get the local config for GeoServer
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
		for (String string : computeResults(params)) {
			JCommander.getConsole().println(
					string);
		}
	}

	@Override
	public List<String> computeResults(
			OperationParams params )
			throws Exception {
		Response getWorkspacesResponse = geoserverClient.getWorkspaces();

		ArrayList<String> results = new ArrayList<>();
		if (getWorkspacesResponse.getStatus() == Status.OK.getStatusCode()) {
			results.add("\nList of GeoServer workspaces:");

			JSONObject jsonResponse = JSONObject.fromObject(getWorkspacesResponse.getEntity());

			final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");
			for (int i = 0; i < workspaces.size(); i++) {
				String wsName = workspaces.getJSONObject(
						i).getString(
						"name");
				results.add("  > " + wsName);
			}

			results.add("---\n");
		}
		else {
			results.add("Error getting GeoServer workspace list; code = " + getWorkspacesResponse.getStatus());
		}
		return results;
	}
}
