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

@GeowaveOperation(name = "addcs", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer coverage store")
public class GeoServerAddCoverageStoreCommand extends
		GeoServerCommand<String>
{
	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "workspace name")
	private String workspace = null;

	@Parameter(names = {
		"-cs",
		"--coverageStore"
	}, required = false, description = "coverage store name")
	private String coverageStore = null;

	@Parameter(names = {
		"-histo",
		"--equalizeHistogramOverride"
	}, required = false, description = "This parameter will override the behavior to always perform histogram equalization if a histogram exists.  Valid values are true and false.", arity = 1)
	private Boolean equalizeHistogramOverride = null;

	@Parameter(names = {
		"-interp",
		"--interpolationOverride"
	}, required = false, description = "This will override the default interpolation stored for each layer.  Valid values are 0, 1, 2, 3 for NearestNeighbor, Bilinear, Bicubic, and Bicubic (polynomial variant) resepctively. ")
	private String interpolationOverride = null;

	@Parameter(names = {
		"-scale",
		"--scaleTo8Bit"
	}, required = false, description = "By default, integer values will automatically be scaled to 8-bit and floating point values will not.  This can be overridden setting this value to true or false.", arity = 1)
	private Boolean scaleTo8Bit = null;

	@Parameter(description = "<GeoWave store name>")
	private List<String> parameters = new ArrayList<String>();
	private String gwStore = null;

	private ServiceStatus status = ServiceStatus.OK;

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
					"Requires argument: <GeoWave store name>");
		}

		gwStore = parameters.get(0);

		if ((workspace == null) || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		final Response addStoreResponse = geoserverClient.addCoverageStore(
				workspace,
				coverageStore,
				gwStore,
				equalizeHistogramOverride,
				interpolationOverride,
				scaleTo8Bit);

		if ((addStoreResponse.getStatus() == Status.OK.getStatusCode())
				|| (addStoreResponse.getStatus() == Status.CREATED.getStatusCode())) {
			setStatus(ServiceStatus.OK);
			return "Add coverage store for '" + gwStore + "' to workspace '" + workspace + "' on GeoServer: OK";
		}
		switch (addStoreResponse.getStatus()) {
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
		return "Error adding coverage store for '" + gwStore + "' to workspace '" + workspace
				+ "' on GeoServer; code = " + addStoreResponse.getStatus();
	}
}
