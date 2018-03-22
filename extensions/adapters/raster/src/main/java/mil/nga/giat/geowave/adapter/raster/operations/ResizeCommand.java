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
package mil.nga.giat.geowave.adapter.raster.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.adapter.raster.operations.options.RasterTileResizeCommandLineOptions;
import mil.nga.giat.geowave.adapter.raster.resize.RasterTileResizeJobRunner;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;

@GeowaveOperation(name = "resize", parentOperation = RasterSection.class)
@Parameters(commandDescription = "Resize Raster Tiles")
public class ResizeCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<input store name> <output store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private RasterTileResizeCommandLineOptions options = new RasterTileResizeCommandLineOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	private DataStorePluginOptions outputStoreOptions = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		createRunner(
				params).runJob();
	}

	public RasterTileResizeJobRunner createRunner(
			final OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <input store name> <output store name>");
		}

		final String inputStoreName = parameters.get(0);
		final String outputStoreName = parameters.get(1);

		// Config file
		final File configFile = getGeoWaveConfigFile(params);
		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Attempt to load output store.
		if (outputStoreOptions == null) {
			final StoreLoader outputStoreLoader = new StoreLoader(
					outputStoreName);
			if (!outputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + outputStoreLoader.getStoreName());
			}
			outputStoreOptions = outputStoreLoader.getDataStorePlugin();
		}

		if (options.getHdfsHostPort() == null) {

			Properties configProperties = ConfigOptions.loadProperties(configFile);
			String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
			options.setHdfsHostPort(hdfsFSUrl);
		}

		RasterTileResizeJobRunner runner = new RasterTileResizeJobRunner(
				inputStoreOptions,
				outputStoreOptions,
				options);
		return runner;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String inputStore,
			final String outputStore ) {
		parameters = new ArrayList<String>();
		parameters.add(inputStore);
		parameters.add(outputStore);
	}

	public RasterTileResizeCommandLineOptions getOptions() {
		return options;
	}

	public void setOptions(
			final RasterTileResizeCommandLineOptions options ) {
		this.options = options;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	public DataStorePluginOptions getOutputStoreOptions() {
		return outputStoreOptions;
	}

	public void setOutputStoreOptions(
			final DataStorePluginOptions outputStoreOptions ) {
		this.outputStoreOptions = outputStoreOptions;
	}
}
