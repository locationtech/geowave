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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "listws", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer workspaces")
public class GeoServerListWorkspacesCommand extends
		ServiceEnabledCommand<List<String>>
{
	private GeoServerRestClient geoserverClient = null;

	@Override
	public boolean prepare(
			final OperationParams params ) {
		// Get the local config for GeoServer
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
		for (final String string : computeResults(params)) {
			JCommander.getConsole().println(
					string);
		}
	}

	@Override
	public List<String> computeResults(
			final OperationParams params )
			throws Exception {
		final Response getWorkspacesResponse = geoserverClient.getWorkspaces();

		final ArrayList<String> results = new ArrayList<>();
		if (getWorkspacesResponse.getStatus() == Status.OK.getStatusCode()) {
			results.add("\nList of GeoServer workspaces:");

			final JSONObject jsonResponse = JSONObject.fromObject(getWorkspacesResponse.getEntity());

			final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");
			for (int i = 0; i < workspaces.size(); i++) {
				final String wsName = workspaces.getJSONObject(
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
