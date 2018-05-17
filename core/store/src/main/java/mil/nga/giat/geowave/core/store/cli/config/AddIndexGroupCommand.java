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
package mil.nga.giat.geowave.core.store.cli.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import mil.nga.giat.geowave.core.cli.exceptions.DuplicateEntryException;
import mil.nga.giat.geowave.core.cli.exceptions.TargetNotFoundException;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.IndexGroupPluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.IndexPluginOptions;

@GeowaveOperation(name = "addindexgrp", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create an index group for usage in GeoWave")
public class AddIndexGroupCommand extends
		ServiceEnabledCommand<String>
{
	@Parameter(description = "<name> <comma separated list of indexes>")
	private List<String> parameters = new ArrayList<String>();

	private final static Logger LOGGER = LoggerFactory.getLogger(AddIndexGroupCommand.class);

	@Override
	public void execute(
			final OperationParams params )
			throws Exception {
		addIndexGroup(params);
	}

	/**
	 * Add rest endpoint for the addIndexGroup command. Looks for POST params
	 * with keys 'key' and 'value' to set.
	 *
	 * @return none
	 * @throws Exception
	 */
	@Override
	public String computeResults(
			final OperationParams params )
			throws Exception {

		return addIndexGroup(params);
	}

	/**
	 * Adds index group
	 * 
	 * @return
	 *
	 * @parameters params
	 * @return none
	 * @throws Exception
	 */
	private String addIndexGroup(
			final OperationParams params )
			throws Exception {
		final File propFile = getGeoWaveConfigFile(params);
		final Properties existingProps = ConfigOptions.loadProperties(propFile);

		if (parameters.size() < 2) {
			throw new ParameterException(
					"Must specify index group name and index names (comma separated)");
		}

		// New index group name
		final String[] indexes = parameters.get(
				1).split(
				",");

		// Make sure the existing group doesn't exist.
		final IndexGroupPluginOptions groupOptions = new IndexGroupPluginOptions();
		if (groupOptions.load(
				existingProps,
				getNamespace())) {
			throw new DuplicateEntryException(
					"That index group already exists: " + getPluginName());
		}

		// Make sure all the indexes exist, and add them to the group options.
		for (int i = 0; i < indexes.length; i++) {
			indexes[i] = indexes[i].trim();
			final IndexPluginOptions options = new IndexPluginOptions();
			if (!options.load(
					existingProps,
					IndexPluginOptions.getIndexNamespace(indexes[i]))) {
				throw new TargetNotFoundException(
						"That index does not exist: " + indexes[i]);
			}
			groupOptions.getDimensionalityPlugins().put(
					indexes[i],
					options);
		}

		// Save the group
		groupOptions.save(
				existingProps,
				getNamespace());

		// Write to disk.
		if (!ConfigOptions.writeProperties(
				propFile,
				existingProps)) {
			throw new WritePropertiesException(
					"Write failure");
		}
		StringBuilder builder = new StringBuilder();
		for (Object key : existingProps.keySet()) {
			String[] split = key.toString().split(
					"\\.");
			if (split.length > 1) {
				if (split[1].equals(parameters.get(0))) {
					builder.append(key.toString() + "=" + existingProps.getProperty(key.toString()) + "\n");
				}
			}
		}
		return builder.toString();
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return IndexGroupPluginOptions.getIndexGroupNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String name,
			final String commaSeparatedIndexes ) {
		parameters = new ArrayList<String>();
		parameters.add(name);
		parameters.add(commaSeparatedIndexes);
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
