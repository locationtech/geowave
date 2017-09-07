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
package mil.nga.giat.geowave.datastore.hbase.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.annotations.RestParameters;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.converters.PasswordConverter;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.config.AddStoreCommand;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;

@GeowaveOperation(name = "addstore/hbase", parentOperation = ConfigSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "Create a store within Geowave")
public class AddHBaseStoreSwaggerCommand extends
		DefaultOperation<Void> implements
		Command
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AddStoreCommand.class);

	public static final String PROPERTIES_CONTEXT = "properties";

	//Default AddStore Options
	@Parameter(description = "<name>")
	@RestParameters(names = {
		"name"
	})
	private List<String> parameters = new ArrayList<String>();


	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default store in all operations")
	private Boolean makeDefault;
	
	private DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
	
	@ParametersDelegate
	private HBaseRequiredOptions requiredOptions;

	@Override
	public boolean prepare(
			final OperationParams params ) {

		// Load SPI options for the given type into pluginOptions.
		// if (storeType != null) {
		// if (storeType.equals("memory")) {
		// GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
		// storeType,
		// new MemoryStoreFactoryFamily());
		// }
		// pluginOptions.selectPlugin(storeType);
		// }
		// else {
		// // Try to load the 'default' options.
		//
		// final File configFile = (File) params.getContext().get(
		// ConfigOptions.PROPERTIES_FILE_CONTEXT);
		// final Properties existingProps = ConfigOptions.loadProperties(
		// configFile,
		// null);
		//
		// final String defaultStore =
		// existingProps.getProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE);
		//
		// // Load the default index.
		// if (defaultStore != null) {
		// try {
		// if (pluginOptions.load(
		// existingProps,
		// DataStorePluginOptions.getStoreNamespace(defaultStore))) {
		// // Set the required type option.
		// storeType = pluginOptions.getType();
		// }
		// }
		// catch (final ParameterException pe) {
		// LOGGER.warn(
		// "Couldn't load default store: " + defaultStore,
		// pe);
		// }
		// }
		// }

		pluginOptions.selectPlugin("hbase");
		pluginOptions.setFactoryOptions(requiredOptions);
		
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

		final File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		final Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

		// Ensure that a name is chosen.
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		final DataStorePluginOptions existingOptions = new DataStorePluginOptions();
		if (existingOptions.load(
				existingProps,
				getNamespace())) {
			throw new ParameterException(
					"That store already exists: " + getPluginName());
		}

		// Save the store options.
		pluginOptions.save(
				existingProps,
				getNamespace());

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					getPluginName());
		}

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);

		return null;
	}

	public DataStorePluginOptions getPluginOptions() {
		return pluginOptions;
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return DataStorePluginOptions.getStoreNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName ) {
		parameters = new ArrayList<String>();
		parameters.add(storeName);
	}

	public Boolean getMakeDefault() {
		return makeDefault;
	}

	public void setMakeDefault(
			final Boolean makeDefault ) {
		this.makeDefault = makeDefault;
	}

	public String getStoreType() {
		return "hbase";
	}

	public void setPluginOptions(
			final DataStorePluginOptions pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}
}