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
package mil.nga.giat.geowave.core.store.operations.remote;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "listindex", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Display all indices in this remote store")
public class ListIndexCommand extends
		ServiceEnabledCommand<String>
{

	private static Logger LOGGER = LoggerFactory.getLogger(RecalculateStatsCommand.class);

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			final OperationParams params ) {
		JCommander.getConsole().println(
				computeResults(params));
	}

	@Override
	public String computeResults(
			final OperationParams params ) {
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
			result = "Cannot find store name: " + inputStoreLoader.getStoreName();
		}
		else {

			// Now that store is loaded, pull the list of indexes

			final DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();

			final CloseableIterator<Index<?, ?>> it = inputStoreOptions.createIndexStore().getIndices();
			final StringBuffer buffer = new StringBuffer();
			while (it.hasNext()) {
				final Index<?, ?> index = it.next();
				buffer.append(
						index.getId().getString()).append(
						' ');
			}
			try {
				it.close();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to close Iterator",
						e);
			}
			result = "Available indexes: " + buffer.toString();
		}
		return result;
	}
}
