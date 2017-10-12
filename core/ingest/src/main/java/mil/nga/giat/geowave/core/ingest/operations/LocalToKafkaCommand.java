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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.ingest.kafka.KafkaProducerCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.kafka.StageToKafkaDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;

@GeowaveOperation(name = "localToKafka", parentOperation = IngestSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "Stage supported files in local file system to a Kafka topic")
public class LocalToKafkaCommand extends
		DefaultOperation<Void> implements
		Command
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
			OperationParams params ) {

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
			OperationParams params )
			throws Exception {
		computeResults(params);
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String fileOrDirectory ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(fileOrDirectory);
	}

	public KafkaProducerCommandLineOptions getKafkaOptions() {
		return kafkaOptions;
	}

	public void setKafkaOptions(
			KafkaProducerCommandLineOptions kafkaOptions ) {
		this.kafkaOptions = kafkaOptions;
	}

	public LocalInputCommandLineOptions getLocalInputOptions() {
		return localInputOptions;
	}

	public void setLocalInputOptions(
			LocalInputCommandLineOptions localInputOptions ) {
		this.localInputOptions = localInputOptions;
	}

	public IngestFormatPluginOptions getPluginFormats() {
		return pluginFormats;
	}

	public void setPluginFormats(
			IngestFormatPluginOptions pluginFormats ) {
		this.pluginFormats = pluginFormats;
	}

	@Override
	public Void computeResults(
			OperationParams params )
			throws Exception {
		// Ensure we have all the required arguments
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires arguments: <file or directory>");
		}

		String inputPath = parameters.get(0);

		// Ingest Plugins
		Map<String, LocalFileIngestPlugin<?>> ingestPlugins = pluginFormats.createLocalIngestPlugins();

		// Driver
		StageToKafkaDriver driver = new StageToKafkaDriver(
				kafkaOptions,
				ingestPlugins,
				localInputOptions);

		// Execute
		if (!driver.runOperation(inputPath)) {
			throw new RuntimeException(
					"Ingest failed to execute");
		}
		return null;
	}
}
