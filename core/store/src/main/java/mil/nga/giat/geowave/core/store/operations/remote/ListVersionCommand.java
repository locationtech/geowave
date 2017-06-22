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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

/**
 * Command for trying to retrieve the version of GeoWave for a remote datastore
 */
@GeowaveOperation(name = "listversion", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "List the version of GeoWave running on the instance of a remote datastore")
public class ListVersionCommand extends
		DefaultOperation implements
		Command
{
	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	@Override
	public void execute(
			OperationParams params )
			throws IOException {

		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		String inputStoreName = parameters.get(0);

		StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
			JCommander.getConsole().println(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
			return;
		}

		DataStorePluginOptions inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		if (inputStoreOptions != null) {
			StoreFactoryOptions factoryOptions = inputStoreOptions.getFactoryOptions();
			DataStore dataStore = inputStoreOptions.createDataStore();

			String version = null;
			if (dataStore instanceof BaseDataStore) {
				JCommander.getConsole().println(
						"Looking up remote datastore version for type [" + inputStoreOptions.getType() + "] and name ["
								+ inputStoreName + "]");
				BaseDataStore baseDataStore = (BaseDataStore) dataStore;
				version = baseDataStore.getVersion(factoryOptions);
			}
			JCommander.getConsole().println(
					"Version: " + version);
		}
	}
}
