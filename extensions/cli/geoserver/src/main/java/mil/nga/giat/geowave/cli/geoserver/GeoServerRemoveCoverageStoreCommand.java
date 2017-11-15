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

@GeowaveOperation(name = "rmcs", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer Coverage Store")
public class GeoServerRemoveCoverageStoreCommand extends
		GeoServerCommand<String>
{
	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "Workspace Name")
	private String workspace;

	@Parameter(description = "<coverage store name>")
	private List<String> parameters = new ArrayList<String>();
	private String cvgstoreName = null;

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

		cvgstoreName = parameters.get(0);

		final Response deleteCvgStoreResponse = geoserverClient.deleteCoverageStore(
				workspace,
				cvgstoreName);

		if (deleteCvgStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			return "Delete store '" + cvgstoreName + "' from workspace '" + workspace + "' on GeoServer: OK";
		}
		return "Error deleting store '" + cvgstoreName + "' from workspace '" + workspace + "' on GeoServer; code = "
				+ deleteCvgStoreResponse.getStatus();
	}
}
