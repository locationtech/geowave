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
package mil.nga.giat.geowave.core.ingest.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.ingest.avro.AvroFormatPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.StageToHdfsDriver;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsDriver;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.MapReduceCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;

@GeowaveOperation(name = "localToMrGW", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Copy supported files from local file system to HDFS and ingest from HDFS")
public class LocalToMapReduceToGeowaveCommand extends
		ServiceEnabledCommand<Void>
{

	@Parameter(description = "<file or directory> <path to base directory to write to> <store name> <comma delimited index/group list>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private VisibilityOptions ingestOptions = new VisibilityOptions();

	@ParametersDelegate
	private MapReduceCommandLineOptions mapReduceOptions = new MapReduceCommandLineOptions();

	@ParametersDelegate
	private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

	// This helper is used to load the list of format SPI plugins that will be
	// used
	@ParametersDelegate
	private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

	private DataStorePluginOptions inputStoreOptions = null;

	private List<IndexPluginOptions> inputIndexOptions = null;

	@Override
	public boolean prepare(
			final OperationParams params ) {

		// Based on the selected formats, select the format plugins
		pluginFormats.selectPlugin(localInputOptions.getFormats());

		return true;
	}

	/**
	 * Prep the driver & run the operation.
	 *
	 * @throws Exception
	 */
	@Override
	public void execute(
			final OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 4) {
			throw new ParameterException(
					"Requires arguments: <file or directory> <path to base directory to write to> <store name> <comma delimited index/group list>");
		}

		computeResults(params);
	}

	@Override
	public boolean runAsync() {
		return true;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String fileOrDirectory,
			final String pathToBaseDirectory,
			final String storeName,
			final String indexList ) {
		parameters = new ArrayList<String>();
		parameters.add(fileOrDirectory);
		parameters.add(pathToBaseDirectory);
		parameters.add(storeName);
		parameters.add(indexList);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	public List<IndexPluginOptions> getInputIndexOptions() {
		return inputIndexOptions;
	}

	public void setInputIndexOptions(
			final List<IndexPluginOptions> inputIndexOptions ) {
		this.inputIndexOptions = inputIndexOptions;
	}

	public VisibilityOptions getIngestOptions() {
		return ingestOptions;
	}

	public void setIngestOptions(
			final VisibilityOptions ingestOptions ) {
		this.ingestOptions = ingestOptions;
	}

	public MapReduceCommandLineOptions getMapReduceOptions() {
		return mapReduceOptions;
	}

	public void setMapReduceOptions(
			final MapReduceCommandLineOptions mapReduceOptions ) {
		this.mapReduceOptions = mapReduceOptions;
	}

	public LocalInputCommandLineOptions getLocalInputOptions() {
		return localInputOptions;
	}

	public void setLocalInputOptions(
			final LocalInputCommandLineOptions localInputOptions ) {
		this.localInputOptions = localInputOptions;
	}

	public IngestFormatPluginOptions getPluginFormats() {
		return pluginFormats;
	}

	public void setPluginFormats(
			final IngestFormatPluginOptions pluginFormats ) {
		this.pluginFormats = pluginFormats;
	}

	@Override
	public Void computeResults(
			final OperationParams params )
			throws Exception {
		if (mapReduceOptions.getJobTrackerOrResourceManagerHostPort() == null) {
			throw new ParameterException(
					"Requires job tracker or resource manager option (try geowave help <command>...)");
		}

		final String inputPath = parameters.get(0);
		String basePath = parameters.get(1);
		String inputStoreName = parameters.get(2);
		String indexList = parameters.get(3);

		// Config file
		File configFile = getGeoWaveConfigFile(params);
		Properties configProperties = ConfigOptions.loadProperties(configFile);
		String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);

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

		// Load the Indexes
		if (inputIndexOptions == null) {
			final IndexLoader indexLoader = new IndexLoader(
					indexList);
			if (!indexLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + indexList);
			}
			inputIndexOptions = indexLoader.getLoadedIndexes();
		}

		// Ingest Plugins
		final Map<String, AvroFormatPlugin<?, ?>> avroIngestPlugins = pluginFormats.createAvroPlugins();

		// Ingest Plugins
		final Map<String, IngestFromHdfsPlugin<?, ?>> hdfsIngestPlugins = pluginFormats.createHdfsIngestPlugins();

		{

			// Driver
			final StageToHdfsDriver driver = new StageToHdfsDriver(
					avroIngestPlugins,
					hdfsHostPort,
					basePath,
					localInputOptions);

			// Execute
			if (!driver.runOperation(
					inputPath,
					configFile)) {
				throw new RuntimeException(
						"Ingest failed to execute");
			}
		}

		{
			// Driver
			final IngestFromHdfsDriver driver = new IngestFromHdfsDriver(
					inputStoreOptions,
					inputIndexOptions,
					ingestOptions,
					mapReduceOptions,
					hdfsIngestPlugins,
					hdfsHostPort,
					basePath);

			// Execute
			if (!driver.runOperation()) {
				throw new RuntimeException(
						"Ingest failed to execute");
			}
		}
		return null;
	};
}
