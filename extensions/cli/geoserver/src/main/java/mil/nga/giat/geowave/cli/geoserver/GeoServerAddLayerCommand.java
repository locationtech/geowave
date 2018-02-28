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
import java.util.Locale;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import net.sf.json.JSONObject;

@GeowaveOperation(name = "addlayer", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Add a GeoServer layer from the given GeoWave store")
public class GeoServerAddLayerCommand extends
		GeoServerCommand<String>
{
	public static enum AddOption {
		ALL,
		RASTER,
		VECTOR;
	}

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "workspace name")
	private String workspace = null;

	@Parameter(names = {
		"-a",
		"--add"
	}, converter = AddOptionConverter.class, description = "For multiple layers, add (all | raster | vector)")
	private AddOption addOption = null;

	@Parameter(names = {
		"-id",
		"--adapterId"
	}, description = "select just <adapter id> from the store")
	private String adapterId = null;

	@Parameter(names = {
		"-sld",
		"--setStyle"
	}, description = "default style sld")
	private String style = null;

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

	public static class AddOptionConverter implements
			IStringConverter<AddOption>
	{
		@Override
		public AddOption convert(
				final String value ) {
			final AddOption convertedValue = AddOption.valueOf(value.toUpperCase());

			if ((convertedValue != AddOption.ALL) && (convertedValue != AddOption.RASTER)
					&& (convertedValue != AddOption.VECTOR)) {
				throw new ParameterException(
						"Value " + value + "can not be converted to an add option. " + "Available values are: "
								+ StringUtils.join(
										AddOption.values(),
										", ").toLowerCase(
										Locale.ENGLISH));
			}
			return convertedValue;
		}
	}

	@Override
	public String computeResults(
			final OperationParams params ) {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <store name>");
		}

		gwStore = parameters.get(0);

		if ((workspace == null) || workspace.isEmpty()) {
			workspace = geoserverClient.getConfig().getWorkspace();
		}

		if (addOption != null) { // add all supercedes specific adapter
									// selection
			adapterId = addOption.name();
		}

		final Response addLayerResponse = geoserverClient.addLayer(
				workspace,
				gwStore,
				adapterId,
				style);

		if (addLayerResponse.getStatus() == Status.OK.getStatusCode()) {
			setStatus(ServiceStatus.OK);
			final JSONObject jsonResponse = JSONObject.fromObject(addLayerResponse.getEntity());
			return "Add GeoServer layer for '" + gwStore + ": OK : " + jsonResponse.toString(2);
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
		return "Error adding GeoServer layer for store '" + gwStore + "; code = " + addLayerResponse.getStatus();
	}
}
