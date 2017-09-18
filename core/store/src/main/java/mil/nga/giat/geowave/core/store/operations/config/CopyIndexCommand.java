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
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;

@GeowaveOperation(name = "cpindex", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Copy and modify existing index configuration")
public class CopyIndexCommand extends
		ServiceEnabledCommand<Void>
{
	@Parameter(description = "<name> <new name>")
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default index creating stores")
	private Boolean makeDefault;

	@ParametersDelegate
	private final IndexPluginOptions newPluginOptions = new IndexPluginOptions();

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

		// Load the old index, so that we can override the values
		String oldIndex = null;
		if (parameters.size() >= 1) {
			oldIndex = parameters.get(0);
			if (!newPluginOptions.load(
					existingProps,
					IndexPluginOptions.getIndexNamespace(oldIndex))) {
				throw new ParameterException(
						"Could not find index: " + oldIndex);
			}
		}

		// Successfully prepared.
		return true;
	}

	@Override
	public void execute(
			final OperationParams params ) {
		copyIndex(params);

	}

	@Override
	public Void computeResults(
			final OperationParams params ) {

		try {
			copyIndex(params);
		}
		catch (WritePropertiesException | ParameterException e) {
			// TODO GEOWAVE-rest-project server error status message
			// this.setStatus(
			// Status.SERVER_ERROR_INTERNAL,
			// e.getMessage());
		}

		return null;
	}

	/**
	 * copies index
	 *
	 * @return none
	 */
	private void copyIndex(
			final OperationParams params ) {

		if (parameters.size() < 2) {
			throw new ParameterException(
					"Must specify <existing index> <new index> names");
		}

		// This is the new index name.
		final String newIndex = parameters.get(1);
		final String newIndexNamespace = IndexPluginOptions.getIndexNamespace(newIndex);

		// Make sure we're not already in the index.
		final IndexPluginOptions existPlugin = new IndexPluginOptions();
		if (existPlugin.load(
				existingProps,
				newIndexNamespace)) {
			throw new ParameterException(
					"That index already exists: " + newIndex);
		}

		// Save the options.
		newPluginOptions.save(
				existingProps,
				newIndexNamespace);

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					IndexPluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					newIndex);
		}

		// Write properties file
		if (!ConfigOptions.writeProperties(
				configFile,
				existingProps)) {
			throw new WritePropertiesException(
					"Write failure");
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String existingIndex,
			final String newIndex ) {
		parameters = new ArrayList<String>();
		parameters.add(existingIndex);
		parameters.add(newIndex);
	}

	private static class WritePropertiesException extends
			RuntimeException
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		private WritePropertiesException(
				final String string ) {
			super(
					string);
		}

	}

}
