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
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;

@GeowaveOperation(name = "addcv", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer coverage")
public class GeoServerAddCoverageCommand extends
		GeoServerCommand<String>
{
	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "workspace name")
	private String workspace = null;

	private ServiceStatus status = ServiceStatus.OK;

	@Parameter(names = {
		"-cs",
		"--cvgstore"
	}, required = true, description = "coverage store name")
	private String cvgstore = null;

	@Parameter(description = "<coverage name>")
	private List<String> parameters = new ArrayList<String>();
	private String cvgName = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		JCommander.getConsole().println(
				computeResults(params));
	}

	public void setStatus(
			ServiceStatus status ) {
		this.status = status;
	}

	@Override
	public Pair<ServiceStatus, String> executeService(
			OperationParams params )
			throws Exception {
		String ret = computeResults(params);
		return ImmutablePair.of(
				status,
				ret);
	}

	@Override
	public String computeResults(
			final OperationParams params ) {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <coverage name>");
		}

		if ((workspace == null) || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		cvgName = parameters.get(0);

		final Response addLayerResponse = geoserverClient.addCoverage(
				workspace,
				cvgstore,
				cvgName);

		if (addLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			setStatus(ServiceStatus.OK);
			return "Add coverage '" + cvgName + "' to '" + workspace + "/" + cvgstore + "' on GeoServer: OK";
		}
		switch (addLayerResponse.getStatus()) {
			case 404:
				setStatus(ServiceStatus.NOT_FOUND);
				break;
			case 400:
				setStatus(ServiceStatus.DUPLICATE);
				break;
			default:
				setStatus(ServiceStatus.INTERNAL_ERROR);
				break;
		}
		return "Error adding GeoServer coverage " + cvgName + "; code = " + addLayerResponse.getStatus();
	}
}
