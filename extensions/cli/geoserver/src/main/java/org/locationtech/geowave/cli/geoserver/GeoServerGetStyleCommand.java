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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.IOUtils;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "getstyle", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Get GeoServer Style info")
public class GeoServerGetStyleCommand extends
		GeoServerCommand<String>
{
	@Parameter(description = "<style name>")
	private List<String> parameters = new ArrayList<String>();
	private String style = null;

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
					"Requires argument: <style name>");
		}
		style = parameters.get(0);

		final Response getStyleResponse = geoserverClient.getStyle(
				style,
				false);

		if (getStyleResponse.getStatus() == Status.OK.getStatusCode()) {
			final String styleInfo = IOUtils.toString((InputStream) getStyleResponse.getEntity());
			return "\nGeoServer style info for '" + style + "': " + styleInfo;
		}
		final String errorMessage = "Error getting GeoServer style info for '" + style + "': "
				+ getStyleResponse.readEntity(String.class) + "\nGeoServer Response Code = "
				+ getStyleResponse.getStatus();
		return handleError(
				getStyleResponse,
				errorMessage);
	}
}
