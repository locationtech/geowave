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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

@GeowaveOperation(name = "cpstore", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Copy and modify existing store configuration")
public class CopyStoreCommand extends
		ServiceEnabledCommand<Void>
{

	@Parameter(description = "<name> <new name>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default store in all operations")
	private Boolean makeDefault;

	@ParametersDelegate
	private final DataStorePluginOptions newPluginOptions = new DataStorePluginOptions();

	private File configFile;
	private Properties existingProps;

	@Override
	public boolean prepare(
			final OperationParams params ) {

		configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		existingProps = ConfigOptions.loadProperties(
				configFile,
				null);

		// Load the old store, so that we can override the values
		String oldStore = null;
		if (parameters.size() >= 1) {
			oldStore = parameters.get(0);
			if (!newPluginOptions.load(
					existingProps,
					DataStorePluginOptions.getStoreNamespace(oldStore))) {
				throw new ParameterException(
						"Could not find store: " + oldStore);
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

	@Override
	public Void computeResults(
			final OperationParams params ) {

		if (parameters.size() < 2) {
			throw new ParameterException(
					"Must specify <existing store> <new store> names");
		}

		// This is the new store name.
		final String newStore = parameters.get(1);
		final String newStoreNamespace = DataStorePluginOptions.getStoreNamespace(newStore);

		// Make sure we're not already in the index.
		final DataStorePluginOptions existPlugin = new DataStorePluginOptions();
		if (existPlugin.load(
				existingProps,
				newStoreNamespace)) {
			throw new ParameterException(
					"That store already exists: " + newStore);
		}

		// Save the options.
		newPluginOptions.save(
				existingProps,
				newStoreNamespace);

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					newStore);
		}

		// Write properties file
		ConfigOptions.writeProperties(
				configFile,
				existingProps);

		return null;

	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String existingStore,
			final String newStore ) {
		parameters = new ArrayList<String>();
		parameters.add(existingStore);
		parameters.add(newStore);
	}
}
