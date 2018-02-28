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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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

@GeowaveOperation(name = "addstyle", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer style")
public class GeoServerAddStyleCommand extends
		GeoServerCommand<String>
{
	@Parameter(names = {
		"-sld",
		"--stylesld"
	}, required = true, description = "style sld file")
	private String stylesld = null;

	@Parameter(description = "<GeoWave style name>")
	private List<String> parameters = new ArrayList<String>();
	private String gwStyle = null;

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
			throws FileNotFoundException,
			IOException {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <style name>");
		}

		gwStyle = parameters.get(0);

		if (gwStyle == null) {
			throw new ParameterException(
					"Requires argument: <style xml file>");
		}

		final File styleXmlFile = new File(
				stylesld);
		try (final FileInputStream inStream = new FileInputStream(
				styleXmlFile)) {
			final Response addStyleResponse = geoserverClient.addStyle(
					gwStyle,
					inStream);

			if ((addStyleResponse.getStatus() == Status.OK.getStatusCode())
					|| (addStyleResponse.getStatus() == Status.CREATED.getStatusCode())) {
				setStatus(ServiceStatus.OK);
				return "Add style for '" + gwStyle + "' on GeoServer: OK";
			}
			switch (addStyleResponse.getStatus()) {
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
			return "Error adding style for '" + gwStyle + "' on GeoServer; code = " + addStyleResponse.getStatus();
		}

	}
}
