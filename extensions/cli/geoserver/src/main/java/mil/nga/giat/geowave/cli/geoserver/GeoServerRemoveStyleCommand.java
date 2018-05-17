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
import mil.nga.giat.geowave.core.cli.exceptions.TargetNotFoundException;

@GeowaveOperation(name = "rmstyle", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Remove GeoServer Style")
public class GeoServerRemoveStyleCommand extends
		GeoServerRemoveCommand<String>
{
	@Parameter(description = "<style name>")
	private List<String> parameters = new ArrayList<String>();
	private String styleName = null;

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

		styleName = parameters.get(0);

		final Response deleteStyleResponse = geoserverClient.deleteStyle(styleName);

		if (deleteStyleResponse.getStatus() == Status.OK.getStatusCode()) {
			return "Delete style '" + styleName + "' on GeoServer: OK";
		}
		String errorMessage = "Error deleting style '" + styleName + "' on GeoServer: "
				+ deleteStyleResponse.readEntity(String.class) + "\nGeoServer Response Code = "
				+ deleteStyleResponse.getStatus();
		return handleError(
				deleteStyleResponse,
				errorMessage);
	}
}
