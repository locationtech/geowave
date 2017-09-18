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
package mil.nga.giat.geowave.core.store.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

@GeowaveOperation(name = "addindex", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Configure an index for usage in GeoWave")
public class AddIndexCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AddIndexCommand.class);

	@Parameter(description = "<name>", required = true)
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default index creating stores")
	private Boolean makeDefault;

	@Parameter(names = {
		"-t",
		"--type"
	}, required = true, description = "The type of index, such as spatial, or spatial_temporal")
	private String type;

	@ParametersDelegate
	private IndexPluginOptions pluginOptions = new IndexPluginOptions();

	@Override
	public boolean prepare(
			final OperationParams params ) {

		// Load SPI options for the given type into pluginOptions.
		if (type != null) {
			pluginOptions.selectPlugin(type);
		}
		else {
			// Try to load the 'default' options.

			final File configFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);
			final Properties existingProps = ConfigOptions.loadProperties(
					configFile,
					null);

			final String defaultIndex = existingProps.getProperty(IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE);

			// Load the default index.
			if (defaultIndex != null) {
				try {
					if (pluginOptions.load(
							existingProps,
							IndexPluginOptions.getIndexNamespace(defaultIndex))) {
						// Set the required type option.
						type = pluginOptions.getType();
					}
				}
				catch (final ParameterException pe) {
					LOGGER.warn(
							"Couldn't load default index: " + defaultIndex,
							pe);
				}
			}
		}

		// Successfully prepared.
		return true;
	}

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}

	public IndexPluginOptions getPluginOptions() {
		return pluginOptions;
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return IndexPluginOptions.getIndexNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String indexName ) {
		parameters = new ArrayList<String>();
		parameters.add(indexName);
	}

	public Boolean getMakeDefault() {
		return makeDefault;
	}

	public void setMakeDefault(
			final Boolean makeDefault ) {
		this.makeDefault = makeDefault;
	}

	public String getType() {
		return type;
	}

	public void setType(
			final String type ) {
		this.type = type;
	}

	public void setPluginOptions(
			final IndexPluginOptions pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}

	public void computeResults(
			final OperationParams params ) {

		// Ensure that a name is chosen.
		if (getParameters().size() < 1) {
			System.out.println(getParameters());
			throw new ParameterException(
					"Must specify index name");
		}

		if (getType() == null) {
			throw new ParameterException(
					"No type could be infered");
		}

		final File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		final Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

		// Make sure we're not already in the index.
		final IndexPluginOptions existPlugin = new IndexPluginOptions();
		if (existPlugin.load(
				existingProps,
				getNamespace())) {
			throw new ParameterException(
					"That index already exists: " + getPluginName());
		}

		final String namespace = getNamespace();
		getPluginOptions().save(
				existingProps,
				namespace);

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {

			existingProps.setProperty(
					IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					getPluginName());
		}

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);
	}
}
