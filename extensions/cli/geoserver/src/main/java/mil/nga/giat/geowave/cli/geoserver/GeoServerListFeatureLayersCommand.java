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

import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "listfl", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "List GeoServer feature layers")
public class GeoServerListFeatureLayersCommand extends
		GeoServerCommand<String>
{
	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "Workspace Name")
	private String workspace = null;

	@Parameter(names = {
		"-ds",
		"--datastore"
	}, required = false, description = "Datastore Name")
	private String datastore = null;

	@Parameter(names = {
		"-g",
		"--geowaveOnly"
	}, required = false, description = "Show only GeoWave feature layers (default: false)")
	private Boolean geowaveOnly = false;

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
			final OperationParams params )
			throws Exception {
		final Response listLayersResponse = geoserverClient.getFeatureLayers(
				workspace,
				datastore,
				geowaveOnly);

		if (listLayersResponse.getStatus() == Status.OK.getStatusCode()) {
			final JSONObject listObj = JSONObject.fromObject(listLayersResponse.getEntity());
			setStatus(ServiceStatus.OK);
			return "\nGeoServer layer list: " + listObj.toString(2);
		}
		switch (listLayersResponse.getStatus()) {
			case 404:
				setStatus(ServiceStatus.NOT_FOUND);
				break;
			default:
				setStatus(ServiceStatus.INTERNAL_ERROR);
				break;
		}
		return "Error getting GeoServer layer list; code = " + listLayersResponse.getStatus();
	}
}
