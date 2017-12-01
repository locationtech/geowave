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
package mil.nga.giat.geowave.cli.osm.operations;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.cli.osm.mapreduce.Convert.OSMConversionRunner;
import mil.nga.giat.geowave.cli.osm.mapreduce.Ingest.OSMRunner;
import mil.nga.giat.geowave.cli.osm.operations.options.OSMIngestCommandArgs;
import mil.nga.giat.geowave.cli.osm.osmfeature.types.features.FeatureDefinitionSet;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;

@GeowaveOperation(name = "ingest", parentOperation = OSMSection.class)
@Parameters(commandDescription = "Ingest and convert OSM data from HDFS to GeoWave")
public class IngestOSMToGeoWaveCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<path to base directory to read from> <store name>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private OSMIngestCommandArgs ingestOptions = new OSMIngestCommandArgs();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <path to base directory to read from> <store name>");
		}

		for (final String string : computeResults(params)) {
			JCommander.getConsole().println(
					string);
		}
	}

	private List<String> ingestData()
			throws Exception {

		final OSMRunner runner = new OSMRunner(
				ingestOptions,
				inputStoreOptions);

		final int res = ToolRunner.run(
				runner,
				new String[] {});
		if (res != 0) {
			throw new RuntimeException(
					"OSMRunner failed: " + res);
		}

		final List<String> output = new ArrayList<>();
		output.add("finished ingest");
		output.add("**************************************************");
		return output;
	}

	private List<String> convertData()
			throws Exception {

		FeatureDefinitionSet.initialize(new OSMIngestCommandArgs().getMappingContents());

		final OSMConversionRunner runner = new OSMConversionRunner(
				ingestOptions,
				inputStoreOptions);

		final int res = ToolRunner.run(
				runner,
				new String[] {});
		if (res != 0) {
			throw new RuntimeException(
					"OSMConversionRunner failed: " + res);
		}

		final List<String> output = new ArrayList<>();
		output.add("finished conversion");
		output.add("**************************************************");
		output.add("**************************************************");
		output.add("**************************************************");
		return output;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String hdfsPath,
			final String storeName ) {
		parameters = new ArrayList<String>();
		parameters.add(hdfsPath);
		parameters.add(storeName);
	}

	public OSMIngestCommandArgs getIngestOptions() {
		return ingestOptions;
	}

	public void setIngestOptions(
			final OSMIngestCommandArgs ingestOptions ) {
		this.ingestOptions = ingestOptions;
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	public List<String> computeResults(
			final OperationParams params )
			throws Exception {
		String basePath = parameters.get(0);
		String inputStoreName = parameters.get(1);

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

		// Copy over options from main parameter to ingest options
		ingestOptions.setHdfsBasePath(basePath);
		ingestOptions.setNameNode(hdfsHostPort);

		if (inputStoreOptions.getGeowaveNamespace() == null) {
			inputStoreOptions.getFactoryOptions().setGeowaveNamespace(
					"osmnamespace");
		}

		if (ingestOptions.getVisibilityOptions().getVisibility() == null) {
			ingestOptions.getVisibilityOptions().setVisibility(
					"public");
		}

		// This is needed by a method in OSMIngsetCommandArgs.
		ingestOptions.setOsmNamespace(inputStoreOptions.getGeowaveNamespace());

		final List<String> outputs = new ArrayList<>();

		// Ingest the data.
		outputs.addAll(ingestData());

		// Convert the data
		outputs.addAll(convertData());

		return outputs;
	}
}
