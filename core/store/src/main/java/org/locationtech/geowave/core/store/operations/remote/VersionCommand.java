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
package org.locationtech.geowave.core.store.operations.remote;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.store.cli.remote.RemoteSection;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.server.ServerSideOperations;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

/**
 * Command for trying to retrieve the version of GeoWave for a remote datastore
 */
@GeowaveOperation(name = "version", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Get the version of GeoWave running on the instance of a remote datastore")
public class VersionCommand extends
		ServiceEnabledCommand<String>
{
	@Parameter(description = "<store name>")
	private final List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		computeResults(params);
	}

	@Override
	public String computeResults(
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
			String ret = "Cannot find store name: " + inputStoreLoader.getStoreName();
			throw new ParameterException(
					ret);
		}

		final DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		// TODO: This return probably should be formatted as JSON
		if (inputStoreOptions != null) {
			DataStoreOperations ops = inputStoreOptions.createDataStoreOperations();
			if (ops instanceof ServerSideOperations
					&& inputStoreOptions.getFactoryOptions().getStoreOptions().isServerSideLibraryEnabled()) {
				JCommander.getConsole().println(
						"Looking up remote datastore version for type [" + inputStoreOptions.getType() + "] and name ["
								+ inputStoreName + "]");
				final String version = "Version: " + ((ServerSideOperations) ops).getVersion();
				JCommander.getConsole().println(
						version);
				return version;
			}
			else {
				String ret1 = "Datastore for type [" + inputStoreOptions.getType() + "] and name [" + inputStoreName
						+ "] does not have a serverside library enabled.";
				JCommander.getConsole().println(
						ret1);
				String ret2 = "Commandline Version: " + VersionUtils.getVersion();
				JCommander.getConsole().println(
						ret2);
				return ret1 + '\n' + ret2;
			}
		}
		return null;
	}

	@Override
	public HttpMethod getMethod() {
		return HttpMethod.GET;
	}
}
