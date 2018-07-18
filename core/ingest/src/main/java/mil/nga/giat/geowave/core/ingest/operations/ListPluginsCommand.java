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
import java.util.Map.Entry;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginProviderSpi;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatPluginRegistry;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeProviderSpi;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeRegistry;

@GeowaveOperation(name = "listplugins", parentOperation = IngestSection.class)
@Parameters(commandDescription = "List supported data store types, index types, and ingest formats")
public class ListPluginsCommand extends
		ServiceEnabledCommand<String>
{

	@Override
	public void execute(
			final OperationParams params ) {
		JCommander.getConsole().println(computeResults(params));
	}

	@Override
	public String computeResults(
			final OperationParams params ) {
		StringBuilder builder = new StringBuilder();
	
		builder.append("Available index types currently registered as plugins:\n");
		for (final Entry<String, DimensionalityTypeProviderSpi> pluginProviderEntry : DimensionalityTypeRegistry
				.getRegisteredDimensionalityTypes()
				.entrySet()) {
			final DimensionalityTypeProviderSpi pluginProvider = pluginProviderEntry.getValue();
			final String desc = pluginProvider.getDimensionalityTypeDescription() == null ? "no description"
					: pluginProvider.getDimensionalityTypeDescription();
			
			builder.append(String.format(
					"%n  %s:%n    %s%n",
					pluginProviderEntry.getKey(),
					desc));
		}

		builder.append("\nAvailable ingest formats currently registered as plugins:\n");
		for (final Entry<String, IngestFormatPluginProviderSpi<?, ?>> pluginProviderEntry : IngestFormatPluginRegistry
				.getPluginProviderRegistry()
				.entrySet()) {
			final IngestFormatPluginProviderSpi<?, ?> pluginProvider = pluginProviderEntry.getValue();
			final String desc = pluginProvider.getIngestFormatDescription() == null ? "no description" : pluginProvider
					.getIngestFormatDescription();
			builder.append(String.format(
					"%n  %s:%n    %s%n",
					pluginProviderEntry.getKey(),
					desc));
		}
		
		builder.append("\nAvailable datastores currently registered:\n");
		final Map<String, StoreFactoryFamilySpi> dataStoreFactories = GeoWaveStoreFinder
				.getRegisteredStoreFactoryFamilies();
		for (final Entry<String, StoreFactoryFamilySpi> dataStoreFactoryEntry : dataStoreFactories.entrySet()) {
			final StoreFactoryFamilySpi dataStoreFactory = dataStoreFactoryEntry.getValue();
			final String desc = dataStoreFactory.getDescription() == null ? "no description" : dataStoreFactory
					.getDescription();
			builder.append(String.format(
					"%n  %s:%n    %s%n",
					dataStoreFactory.getType(),
					desc));
		}
		
		return builder.toString();
	}
}
