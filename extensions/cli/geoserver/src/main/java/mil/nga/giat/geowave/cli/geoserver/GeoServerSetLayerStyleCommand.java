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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.exceptions.TargetNotFoundException;

@GeowaveOperation(name = "setls", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Set GeoServer Layer Style")
public class GeoServerSetLayerStyleCommand extends
		GeoServerCommand<String>
{
	/**
	 * Return "200 OK" for the set layer command.
	 */
	@Override
	public Boolean successStatusIs200() {
		return true;
	}

	@Parameter(names = {
		"-sn",
		"--styleName"
	}, required = true, description = "style name")
	private final String styleName = null;

	@Parameter(description = "<layer name>")
	private List<String> parameters = new ArrayList<String>();
	private String layerName = null;

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
					"Requires argument: <layer name>");
		}

		layerName = parameters.get(0);

		final Response setLayerStyleResponse = geoserverClient.setLayerStyle(
				layerName,
				styleName);

		if (setLayerStyleResponse.getStatus() == Status.OK.getStatusCode()) {
			final String style = IOUtils.toString((InputStream) setLayerStyleResponse.getEntity());
			return "Set style for GeoServer layer '" + layerName + ": OK" + style;

		}
		String errorMessage = "Error setting style for GeoServer layer '" + layerName + "': "
				+ setLayerStyleResponse.readEntity(String.class) + "\nGeoServer Response Code = "
				+ setLayerStyleResponse.getStatus();
		return handleError(
				setLayerStyleResponse,
				errorMessage);
	}
}
