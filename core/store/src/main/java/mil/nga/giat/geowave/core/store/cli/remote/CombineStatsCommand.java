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
package mil.nga.giat.geowave.core.store.cli.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.cli.remote.options.StoreLoader;

@GeowaveOperation(name = "combinestats", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Combine all statistics in GeoWave namespace")
public class CombineStatsCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<storename> <adapter id>")
	private List<String> parameters = new ArrayList<String>();

	private DataStorePluginOptions inputStoreOptions = null;

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {

		// Ensure we have all the required arguments
		if (parameters.size() != 2) {
			throw new ParameterException(
					"Requires arguments: <storename> <adapter id>");
		}

		final String inputStoreName = parameters.get(0);
		final String adapterId = parameters.get(1);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(getGeoWaveConfigFile(params))) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		// Get all statistics, remove all statistics, then re-add
		final DataStatisticsStore store = inputStoreOptions.createDataStatisticsStore();
		final InternalAdapterStore internalAdapterStore = inputStoreOptions.createInternalAdapterStore();
		Short internalId = internalAdapterStore.getInternalAdapterId(new ByteArrayId(
				adapterId));
		if (internalId != null) {
			DataStatistics<?>[] statsArray;
			try (final CloseableIterator<DataStatistics<?>> stats = store.getDataStatistics(internalId)) {
				statsArray = Iterators.toArray(
						stats,
						DataStatistics.class);
			}
			catch (IOException e) {
				// wrap in a parameter exception
				throw new ParameterException(
						"Unable to combine stats",
						e);
			}
			// Clear all existing stats
			store.removeAllStatistics(internalId);
			for (DataStatistics<?> stats : statsArray) {
				store.incorporateStatistics(stats);
			}
		}
		else {
			throw new ParameterException(
					"Cannot find adapter name: " + adapterId);
		}
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName,
			final String adapterId ) {
		parameters = Arrays.asList(
				storeName,
				adapterId);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			final DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}
}