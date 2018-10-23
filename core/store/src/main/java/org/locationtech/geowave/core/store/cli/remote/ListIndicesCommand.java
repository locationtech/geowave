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
package org.locationtech.geowave.core.store.cli.remote;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "listindices", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Display all indices in this remote store")
public class ListIndicesCommand extends
		ServiceEnabledCommand<String>
{
	@Parameter(description = "<store name>")
	private final List<String> parameters = new ArrayList<>();

	@Override
	public void execute(
			final OperationParams params )
			throws TargetNotFoundException {
		JCommander.getConsole().println(
				computeResults(params));
	}

	@Override
	public String computeResults(
			final OperationParams params )
			throws TargetNotFoundException {
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		final String inputStoreName = parameters.get(0);

		// Get the config options from the properties file

		final File configFile = getGeoWaveConfigFile(params);

		// Attempt to load the desired input store

		String result;

		final StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		else {

			// Now that store is loaded, pull the list of indexes

			final DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();

			final StringBuffer buffer = new StringBuffer();
			try (final CloseableIterator<Index> it = inputStoreOptions.createIndexStore().getIndices()) {
				while (it.hasNext()) {
					final Index index = it.next();
					buffer.append(
							index.getName()).append(
							' ');
				}
			}
			result = "Available indexes: " + buffer.toString();
		}
		return result;
	}
}
