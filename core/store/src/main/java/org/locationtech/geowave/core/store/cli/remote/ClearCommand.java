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
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "clear", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Clear ALL data from a GeoWave store and delete tables")
public class ClearCommand extends
		ServiceEnabledCommand<Void>
{

	/**
	 * Return "200 OK" for all clear commands.
	 */
	@Override
	public Boolean successStatusIs200() {
		return true;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(ClearCommand.class);

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<>();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName ) {
		parameters = new ArrayList<>();
		parameters.add(storeName);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	@Override
	public Void computeResults(
			final OperationParams params ) {
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		final String inputStoreName = parameters.get(0);

		// Attempt to load store.
		final File configFile = getGeoWaveConfigFile(params);

		// Attempt to load input store.
		final StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		LOGGER.info("Deleting everything in store: " + inputStoreName);

		inputStoreOptions.createDataStore().delete(
				QueryBuilder.newBuilder().build());
		return null;
	}
}
