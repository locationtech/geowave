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
package org.locationtech.geowave.mapreduce.input;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.core.store.query.options.QueryAllIndices;
import org.locationtech.geowave.core.store.query.options.QueryAllTypes;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextIndexStore;
import org.locationtech.geowave.mapreduce.MapReduceDataStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputConfigurator.InputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveInputFormat<T> extends
		InputFormat<GeoWaveInputKey, T>
{
	private static final Class<?> CLASS = GeoWaveInputFormat.class;
	protected static final Logger LOGGER = LoggerFactory.getLogger(CLASS);

	public static void setStoreOptionsMap(
			final Configuration config,
			final Map<String, String> storeConfigOptions ) {
		GeoWaveConfiguratorBase.setStoreOptionsMap(
				CLASS,
				config,
				storeConfigOptions);
	}

	public static void setStoreOptions(
			final Configuration config,
			final DataStorePluginOptions storeOptions ) {
		if (storeOptions != null) {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					storeOptions.getOptionsAsMap());
		}
		else {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					null);
		}
	}

	public static IndexStore getJobContextIndexStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextIndexStore(
				CLASS,
				context);
	}

	public static AdapterIndexMappingStore getJobContextAdapterIndexMappingStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterIndexMappingStore(
				CLASS,
				context);
	}

	public static TransientAdapterStore getJobContextAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterStore(
				CLASS,
				context);
	}

	public static DataStatisticsStore getJobContextDataStatisticsStore(
			final JobContext context ) {
		// TODO, this doesn't create a data statistics store wrapping a
		// jobcontext as the name implies, need to either wrap a job context or
		// rename this (for adapter and index store, adapters and indices are
		// stored in the job context rather than multiple processes needing to
		// look it up, this doesn't seem to be happening for stats)
		return GeoWaveConfiguratorBase.getDataStatisticsStore(
				CLASS,
				context);
	}

	public static InternalAdapterStore getJobContextInternalAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextInternalAdapterStore(
				CLASS,
				context);
	}

	public static void setMinimumSplitCount(
			final Configuration config,
			final Integer minSplits ) {
		GeoWaveInputConfigurator.setMinimumSplitCount(
				CLASS,
				config,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Configuration config,
			final Integer maxSplits ) {
		GeoWaveInputConfigurator.setMaximumSplitCount(
				CLASS,
				config,
				maxSplits);
	}

	public static void setIsOutputWritable(
			final Configuration config,
			final Boolean isOutputWritable ) {
		config.setBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				isOutputWritable);
	}

	public static void setQuery(
			final Configuration config,
			final Query<?> query,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final IndexStore indexStore ) {
		setCommonQueryOptions(
				config,
				query.getCommonQueryOptions());
		setDataTypeQueryOptions(
				config,
				query.getDataTypeQueryOptions(),
				adapterStore,
				internalAdapterStore);
		setIndexQueryOptions(
				config,
				query.getIndexQueryOptions(),
				indexStore);
		setQueryConstraints(
				config,
				(QueryConstraints) query.getQueryConstraints());
	}

	public static void setQueryConstraints(
			final Configuration config,
			final QueryConstraints query ) {
		GeoWaveInputConfigurator.setQueryConstraints(
				CLASS,
				config,
				query);
	}

	protected static QueryConstraints getQueryConstraints(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getQueryCosntraints(
				CLASS,
				context);
	}

	public static void setIndexQueryOptions(
			final Configuration config,
			final IndexQueryOptions queryOptions,
			final IndexStore indexStore ) {
		final String indexName = queryOptions.getIndexName();
		if (indexName != null) {
			// make available to the context index store
			JobContextIndexStore.addIndex(
					config,
					indexStore.getIndex(indexName));
		}

		GeoWaveInputConfigurator.setIndexQueryOptions(
				CLASS,
				config,
				queryOptions);
	}

	protected static IndexQueryOptions getIndexQueryOptions(
			final JobContext context ) {
		final IndexQueryOptions options = GeoWaveInputConfigurator.getIndexQueryOptions(
				CLASS,
				context);
		return options == null ? new QueryAllIndices() : options;
	}

	protected static DataTypeQueryOptions<?> getDataTypeQueryOptions(
			final JobContext context ) {
		final DataTypeQueryOptions<?> options = GeoWaveInputConfigurator.getDataTypeQueryOptions(
				CLASS,
				context);
		return options == null ? new QueryAllTypes<>() : options;
	}

	public static void setDataTypeQueryOptions(
			final Configuration config,
			final DataTypeQueryOptions<?> queryOptions,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore ) {
		// TODO figure out where to add internal adapter IDs to the job context
		// and read it from the job context instead
		try {
			// THIS SHOULD GO AWAY, and assume the adapters in the Persistent
			// Data Store
			// instead. It will fail, due to the 'null', if the query options
			// does not
			// contain the adapters
			final String[] typeNames = queryOptions.getTypeNames();
			if ((typeNames != null) && (typeNames.length > 0)) {
				for (final String typeName : typeNames) {
					// Also store for use the mapper and reducers
					final Short adapterId = internalAdapterStore.getAdapterId(typeName);
					if (adapterId == null) {
						LOGGER.error("Cannot fine type '" + typeName + "'");
						continue;
					}
					JobContextAdapterStore.addDataAdapter(
							config,
							adapterStore.getAdapter(adapterId));
				}
			}
		}
		catch (final Exception e) {
			LOGGER
					.warn(
							"Adapter Ids witih adapters are included in the query options.This, the adapter must be accessible from the data store for use by the consumer/Mapper.",
							e);
		}
		GeoWaveInputConfigurator.setDataTypeQueryOptions(
				CLASS,
				config,
				queryOptions);
	}

	protected static CommonQueryOptions getCommonQueryOptions(
			final JobContext context ) {
		final CommonQueryOptions options = GeoWaveInputConfigurator.getCommonQueryOptions(
				CLASS,
				context);
		return options == null ? new CommonQueryOptions() : options;
	}

	public static void setCommonQueryOptions(
			final Configuration config,
			final CommonQueryOptions queryOptions ) {
		GeoWaveInputConfigurator.setCommonQueryOptions(
				CLASS,
				config,
				queryOptions);
	}

	protected static Index getIndex(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getIndex(
				CLASS,
				GeoWaveConfiguratorBase.getConfiguration(context));
	}

	protected static Boolean isOutputWritable(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getConfiguration(
				context).getBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				false);
	}

	protected static Integer getMinimumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMinimumSplitCount(
				CLASS,
				context);
	}

	protected static Integer getMaximumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMaximumSplitCount(
				CLASS,
				context);
	}

	@Override
	public RecordReader<GeoWaveInputKey, T> createRecordReader(
			final InputSplit split,
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		final Map<String, String> configOptions = getStoreOptionsMap(context);
		final DataStore dataStore = GeoWaveStoreFinder.createDataStore(configOptions);
		if ((dataStore != null) && (dataStore instanceof MapReduceDataStore)) {
			return (RecordReader<GeoWaveInputKey, T>) ((MapReduceDataStore) dataStore).createRecordReader(
					getCommonQueryOptions(context),
					getDataTypeQueryOptions(context),
					getIndexQueryOptions(context),
					getQueryConstraints(context),
					getJobContextAdapterStore(context),
					getJobContextInternalAdapterStore(context),
					getJobContextAdapterIndexMappingStore(context),
					getJobContextDataStatisticsStore(context),
					getJobContextIndexStore(context),
					isOutputWritable(
							context).booleanValue(),
					split);
		}
		LOGGER.error("Data Store does not support map reduce");
		throw new IOException(
				"Data Store does not support map reduce");
	}

	/**
	 * Check whether a configuration is fully configured to be used with an
	 * Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
	 *
	 * @param context
	 *            the Hadoop context for the configured job
	 * @throws IOException
	 *             if the context is improperly configured
	 * @since 1.5.0
	 */
	protected static void validateOptions(
			final JobContext context )
			throws IOException {// attempt to get each of the GeoWave
								// stores
								// from the job context
		try {
			final Map<String, String> configOptions = getStoreOptionsMap(context);
			final StoreFactoryFamilySpi factoryFamily = GeoWaveStoreFinder.findStoreFamily(configOptions);
			if (factoryFamily == null) {
				final String msg = "Unable to find GeoWave data store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error finding GeoWave stores",
					e);
			throw new IOException(
					"Error finding GeoWave stores",
					e);
		}
	}

	public static DataStorePluginOptions getStoreOptions(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreOptions(
				CLASS,
				context);
	}

	public static Map<String, String> getStoreOptionsMap(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreOptionsMap(
				CLASS,
				context);
	}

	@Override
	public List<InputSplit> getSplits(
			final JobContext context )
			throws IOException,
			InterruptedException {
		final Map<String, String> configOptions = getStoreOptionsMap(context);
		final DataStore dataStore = GeoWaveStoreFinder.createDataStore(configOptions);
		if ((dataStore != null) && (dataStore instanceof MapReduceDataStore)) {
			return ((MapReduceDataStore) dataStore).getSplits(
					getCommonQueryOptions(context),
					getDataTypeQueryOptions(context),
					getIndexQueryOptions(context),
					getQueryConstraints(context),
					getJobContextAdapterStore(context),
					getJobContextAdapterIndexMappingStore(context),
					getJobContextDataStatisticsStore(context),
					getJobContextInternalAdapterStore(context),
					getJobContextIndexStore(context),
					context,
					getMinimumSplitCount(context),
					getMaximumSplitCount(context));
		}

		LOGGER.error("Data Store does not support map reduce");
		throw new IOException(
				"Data Store does not support map reduce");
	}
}
