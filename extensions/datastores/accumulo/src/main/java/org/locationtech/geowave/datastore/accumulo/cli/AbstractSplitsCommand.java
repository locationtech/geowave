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
package org.locationtech.geowave.datastore.accumulo.cli;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.datastore.accumulo.split.SplitCommandLineOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;

public abstract class AbstractSplitsCommand extends
		DefaultOperation
{

	@Parameter(description = "<storename>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	protected SplitCommandLineOptions splitOptions = new SplitCommandLineOptions();

	protected DataStorePluginOptions inputStoreOptions = null;

	public AbstractSplitsCommand() {

	}

	public void execute(
			OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <storename>");
		}

		String inputStoreName = parameters.get(0);

		// Config file
		File configFile = getGeoWaveConfigFile(params);

		StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		doSplit();
	}

	public abstract void doSplit()
			throws Exception;

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public SplitCommandLineOptions getSplitOptions() {
		return splitOptions;
	}

	public void setSplitOptions(
			SplitCommandLineOptions splitOptions ) {
		this.splitOptions = splitOptions;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}
}
