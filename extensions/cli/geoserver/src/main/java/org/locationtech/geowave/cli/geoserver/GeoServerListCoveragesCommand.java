/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.cli.geoserver;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceStatus;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "listcv", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer Coverages")
public class GeoServerListCoveragesCommand extends
		GeoServerCommand<String>
{
	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "workspace name")
	private String workspace;

	@Parameter(description = "<coverage store name>")
	private List<String> parameters = new ArrayList<String>();
	private String csName = null;

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
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <coverage store name>");
		}

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
		String errorMessage = "Error getting GeoServer coverage list for '" + csName + "': "
				+ getCvgStoreResponse.readEntity(String.class) + "\nGeoServer Response Code = "
				+ getCvgStoreResponse.getStatus();
		return handleError(
				getCvgStoreResponse,
				errorMessage);
	}
}
