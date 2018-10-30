/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.cli.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreStatisticsProvider;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatsCompositionTool;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StatsCommandLineOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "calcstat", parentOperation = RemoteSection.class)
@Parameters(commandDescription = "Calculate a specific statistic in the remote store, given adapter ID and statistic ID")
/**
 * This class calculates the statistic(s) in the given store and replaces the
 * existing value.
 */
public class CalculateStatCommand extends
		AbstractStatsCommand<Void>
{

	private static final Logger LOGGER = LoggerFactory.getLogger(CalculateStatCommand.class);

	@Parameter(description = "<store name> <datatype name> <stat type>")
	private List<String> parameters = new ArrayList<>();

	// The state we're re-caculating. Set in execute(), used in
	// calculateStatistics()
	private String statType;

	@Parameter(names = {
		"--fieldName"
	}, description = "If the statistic is maintained per field, optionally provide a field name")
	private String fieldName = "";

	@Override
	public void execute(
			final OperationParams params ) {
		computeResults(params);
	}

	@Override
	protected boolean performStatsCommand(
			final DataStorePluginOptions storeOptions,
			final InternalDataAdapter<?> adapter,
			final StatsCommandLineOptions statsOptions )
			throws IOException {

		try {
			final DataStore dataStore = storeOptions.createDataStore();
			if (!(dataStore instanceof BaseDataStore)) {
				LOGGER.warn("Datastore type '" + dataStore.getClass().getName()
						+ "' must be instance of BaseDataStore to recalculate stats");
				return false;
			}
			final AdapterIndexMappingStore mappingStore = storeOptions.createAdapterIndexMappingStore();
			final IndexStore indexStore = storeOptions.createIndexStore();

			boolean isFirstTime = true;
			for (final Index index : mappingStore.getIndicesForAdapter(
					adapter.getAdapterId()).getIndices(
					indexStore)) {

				@SuppressWarnings({
					"rawtypes",
					"unchecked"
				})
				final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());
				final DataStoreStatisticsProvider provider = new DataStoreStatisticsProvider(
						adapter,
						index,
						isFirstTime) {
					@Override
					public StatisticsId[] getSupportedStatistics() {
						return new StatisticsId[] {
							new StatisticsId(
									new BaseStatisticsType<>(
											statType),
									fieldName)
						};
					}
				};

				try (StatsCompositionTool<?> statsTool = new StatsCompositionTool(
						provider,
						storeOptions.createDataStatisticsStore(),
						index,
						adapter)) {

					try (CloseableIterator<?> entryIt = ((BaseDataStore) dataStore).query(
							QueryBuilder.newBuilder().addTypeName(
									adapter.getTypeName()).indexName(
									index.getName()).setAuthorizations(
									authorizations).build(),
							(ScanCallback) statsTool)) {
						while (entryIt.hasNext()) {
							entryIt.next();
						}
					}

				}
				isFirstTime = false;
			}

		}
		catch (final Exception ex) {
			LOGGER.error(
					"Error while writing statistics.",
					ex);
			return false;
		}

		return true;
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			final String storeName,
			final String dataTypeName,
			final String statType ) {
		parameters = new ArrayList<>();
		parameters.add(storeName);
		parameters.add(dataTypeName);
		parameters.add(statType);
	}

	@Override
	public Void computeResults(
			final OperationParams params ) {
		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <store name> <datatype name> <stat type>");
		}

		statType = parameters.get(2);

		super.run(
				params,
				parameters);
		return null;
	}
}
