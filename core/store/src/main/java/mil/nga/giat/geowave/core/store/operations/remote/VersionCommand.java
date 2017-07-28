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
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.store.cli.remote.RemoteSection;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.server.ServerSideOperations;

/**
 * Command for trying to retrieve the version of GeoWave for a remote datastore
 */
@GeowaveOperation(name = "version", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Get the version of GeoWave running on the instance of a remote datastore")
public class VersionCommand extends
		ServiceEnabledCommand<Void>
{
	@Parameter(description = "<storename>")
	private final List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		computeResults(params);
	}

	@Override
	public Void computeResults(
			final OperationParams params )
			throws Exception {
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		final String inputStoreName = parameters.get(0);

		final File configFile = getGeoWaveConfigFile(params);

		final StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(configFile)) {
			JCommander.getConsole().println(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
			return null;
		}

		final DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		if (inputStoreOptions != null) {
			DataStoreOperations ops = inputStoreOptions.createDataStoreOperations();
			if (ops instanceof ServerSideOperations
					&& inputStoreOptions.getFactoryOptions().getStoreOptions().isServerSideLibraryEnabled()) {
				JCommander.getConsole().println(
						"Looking up remote datastore version for type [" + inputStoreOptions.getType() + "] and name ["
								+ inputStoreName + "]");
				final String version = ((ServerSideOperations) ops).getVersion();
				JCommander.getConsole().println(
						"Version: " + version);
			}
			else {
				JCommander.getConsole().println(
						"Datastore for type [" + inputStoreOptions.getType() + "] and name [" + inputStoreName
								+ "] does not have a serverside library enabled.");
				JCommander.getConsole().println(
						"Commandline Version: " + VersionUtils.getVersion());
			}
		}
		return null;
	}

	@Override
	public HttpMethod getMethod() {
		return HttpMethod.GET;
	}
}
