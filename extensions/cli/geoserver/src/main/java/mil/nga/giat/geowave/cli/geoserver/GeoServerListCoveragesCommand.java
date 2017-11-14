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
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "listcv", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer Coverages")
public class GeoServerListCoveragesCommand extends
		GeoServerCommand<String>
{
	private GeoServerRestClient geoserverClient = null;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "workspace name")
	private String workspace;

	@Parameter(description = "<coverage store name>")
	private final List<String> parameters = new ArrayList<String>();
	private String csName = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <coverage store name>");
		}

		JCommander.getConsole().println(
				computeResults(params));
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws Exception {
		if ((workspace == null) || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		csName = parameters.get(0);

		final Response getCvgStoreResponse = geoserverClient.getCoverages(
				workspace,
				csName);

		if (getCvgStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			final JSONObject jsonResponse = JSONObject.fromObject(getCvgStoreResponse.getEntity());
			final JSONArray cvgArray = jsonResponse.getJSONArray("coverages");
			return "\nGeoServer coverage list for '" + csName + "': " + cvgArray.toString(2);
		}
		return "Error getting GeoServer coverage list for '" + csName + "'; code = " + getCvgStoreResponse.getStatus();
	}
}
