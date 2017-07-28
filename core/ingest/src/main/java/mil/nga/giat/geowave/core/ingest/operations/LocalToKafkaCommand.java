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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.ingest.kafka.KafkaProducerCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.kafka.StageToKafkaDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;

@GeowaveOperation(name = "localToKafka", parentOperation = IngestSection.class)
@Parameters(commandDescription = "Stage supported files in local file system to a Kafka topic")
public class LocalToKafkaCommand extends
		ServiceEnabledCommand<Void>
{

	@Parameter(description = "<file or directory>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private KafkaProducerCommandLineOptions kafkaOptions = new KafkaProducerCommandLineOptions();

	@ParametersDelegate
	private LocalInputCommandLineOptions localInputOptions = new LocalInputCommandLineOptions();

	// This helper is used to load the list of format SPI plugins that will be
	// used
	@ParametersDelegate
	private IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();

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
			final String fileOrDirectory ) {
		parameters = new ArrayList<String>();
		parameters.add(fileOrDirectory);
	}

	public KafkaProducerCommandLineOptions getKafkaOptions() {
		return kafkaOptions;
	}

	public void setKafkaOptions(
			final KafkaProducerCommandLineOptions kafkaOptions ) {
		this.kafkaOptions = kafkaOptions;
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
		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <file or directory>");
		}

		final String inputPath = parameters.get(0);

		// Ingest Plugins
		final Map<String, LocalFileIngestPlugin<?>> ingestPlugins = pluginFormats.createLocalIngestPlugins();

		// Driver
		final StageToKafkaDriver driver = new StageToKafkaDriver(
				kafkaOptions,
				ingestPlugins,
				localInputOptions);

		// Config file
		File configFile = getGeoWaveConfigFile(params);

		// Execute
		if (!driver.runOperation(
				inputPath,
				configFile)) {
			throw new RuntimeException(
					"Ingest failed to execute");
		}
		return null;
	}
}
