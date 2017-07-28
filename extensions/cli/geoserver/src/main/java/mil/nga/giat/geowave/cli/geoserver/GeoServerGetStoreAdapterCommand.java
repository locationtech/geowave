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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "getsa", parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Get GeoWave store adapters")
public class GeoServerGetStoreAdapterCommand extends
		GeoServerCommand<List<String>>
{
	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();
	private String storeName = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		List<String> adapterList = computeResults(params);

		JCommander.getConsole().println(
				"Store " + storeName + " has these adapters:");
		for (final String adapterId : adapterList) {
			JCommander.getConsole().println(
					adapterId);
		}
	}

	@Override
	public List<String> computeResults(
			final OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <store name>");
		}
		storeName = parameters.get(0);
		final List<String> adapterList = geoserverClient.getStoreAdapters(
				storeName,
				null);
		return adapterList;
	}
}
