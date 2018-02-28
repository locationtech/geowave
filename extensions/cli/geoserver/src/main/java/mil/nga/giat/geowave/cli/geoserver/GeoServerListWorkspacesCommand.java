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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "listws", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer workspaces")
public class GeoServerListWorkspacesCommand extends
		GeoServerCommand<List<String>>
{
	private ServiceStatus status = ServiceStatus.OK;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		for (final String string : computeResults(params)) {
			JCommander.getConsole().println(
					string);
		}
	}

	public void setStatus(
			ServiceStatus status ) {
		this.status = status;
	}

	@Override
	public Pair<ServiceStatus, List<String>> executeService(
			OperationParams params )
			throws Exception {
		List<String> ret = computeResults(params);
		return ImmutablePair.of(
				status,
				ret);
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
		switch (getWorkspacesResponse.getStatus()) {
			case 404:
				setStatus(ServiceStatus.NOT_FOUND);
				break;
			default:
				setStatus(ServiceStatus.INTERNAL_ERROR);
				break;
		}
		return results;
	}
}
