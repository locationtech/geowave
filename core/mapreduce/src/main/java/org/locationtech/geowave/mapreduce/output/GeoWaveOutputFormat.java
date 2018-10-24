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
package org.locationtech.geowave.mapreduce.output;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextIndexStore;
import org.locationtech.geowave.mapreduce.MapReduceDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This output format is the preferred mechanism for writing data to GeoWave
 * within a map-reduce job.
 */
public class GeoWaveOutputFormat extends
		OutputFormat<GeoWaveOutputKey<Object>, Object>
{
	private static final Class<?> CLASS = GeoWaveOutputFormat.class;
	protected static final Logger LOGGER = LoggerFactory.getLogger(CLASS);

	@Override
	public RecordWriter<GeoWaveOutputKey<Object>, Object> getRecordWriter(
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		try {
			final Map<String, String> configOptions = getStoreOptionsMap(context);

			final IndexStore persistentIndexStore = GeoWaveStoreFinder.createIndexStore(configOptions);
			final Index[] indices = JobContextIndexStore.getIndices(context);
			if (LOGGER.isDebugEnabled()) {
				final StringBuilder sbDebug = new StringBuilder();

				sbDebug.append("Config Options: ");
				for (final Map.Entry<String, String> entry : configOptions.entrySet()) {
					sbDebug.append(entry.getKey() + "/" + entry.getValue() + ", ");
				}
				sbDebug.append("\n\tIndices Size: " + indices.length);
				sbDebug.append("\n\tpersistentIndexStore: " + persistentIndexStore);
				final String filename = "/META-INF/services/org.locationtech.geowave.core.store.StoreFactoryFamilySpi";

				final InputStream is = context.getClass().getResourceAsStream(
						filename);
				if (is == null) {
					sbDebug.append("\n\tStoreFactoryFamilySpi: Unable to open file '" + filename + "'");
				}
				else {
					sbDebug.append("\n\tStoreFactoryFamilySpi: " + IOUtils.toString(
							is,
							"UTF-8"));
					is.close();
				}

				LOGGER.debug(sbDebug.toString());
			}

			for (final Index i : indices) {
				if (!persistentIndexStore.indexExists(i.getName())) {
					persistentIndexStore.addIndex(i);
				}
			}
			final TransientAdapterStore jobContextAdapterStore = GeoWaveConfiguratorBase.getJobContextAdapterStore(
					CLASS,
					context);
			final IndexStore jobContextIndexStore = new JobContextIndexStore(
					context,
					persistentIndexStore);
			final DataStore dataStore = GeoWaveStoreFinder.createDataStore(configOptions);
			return new GeoWaveRecordWriter(
					context,
					dataStore,
					jobContextIndexStore,
					jobContextAdapterStore);
		}
		catch (final Exception e) {
			throw new IOException(
					e);
		}
	}

	public static void setStoreOptions(
			final Configuration config,
			final DataStorePluginOptions storeOptions ) {
		if (storeOptions != null) {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					storeOptions.getOptionsAsMap());
			final DataStore dataStore = storeOptions.createDataStore();
			if ((dataStore != null) && (dataStore instanceof MapReduceDataStore)) {
				((MapReduceDataStore) dataStore).prepareRecordWriter(config);
			}
		}
		else {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					null);
		}
	}

	public static void setStoreOptionsMap(
			final Configuration config,
			final Map<String, String> storeConfigOptions ) {
		GeoWaveConfiguratorBase.setStoreOptionsMap(
				CLASS,
				config,
				storeConfigOptions);
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextIndexStore.addIndex(
				config,
				index);
	}

	public static void addDataAdapter(
			final Configuration config,
			final DataTypeAdapter<?> adapter ) {
		JobContextAdapterStore.addDataAdapter(
				config,
				adapter);
	}

	public static IndexStore getJobContextIndexStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextIndexStore(
				CLASS,
				context);
	}

	public static AdapterStore getJobContextAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterStore(
				CLASS,
				context);
	}

	public static AdapterIndexMappingStore getJobContextAdapterIndexMappingStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterIndexMappingStore(
				CLASS,
				context);
	}

	public static InternalAdapterStore getJobContextInternalAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextInternalAdapterStore(
				CLASS,
				context);
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
	public void checkOutputSpecs(
			final JobContext context )
			throws IOException,
			InterruptedException {
		// attempt to get each of the GeoWave stores from the job context
		try {
			final Map<String, String> configOptions = getStoreOptionsMap(context);
			if (GeoWaveStoreFinder.createDataStore(configOptions) == null) {
				final String msg = "Unable to find GeoWave data store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createIndexStore(configOptions) == null) {
				final String msg = "Unable to find GeoWave index store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createAdapterStore(configOptions) == null) {
				final String msg = "Unable to find GeoWave adapter store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createDataStatisticsStore(configOptions) == null) {
				final String msg = "Unable to find GeoWave data statistics store";
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

	@Override
	public OutputCommitter getOutputCommitter(
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		return new NullOutputFormat<ByteArray, Object>().getOutputCommitter(context);
	}

	/**
	 * A base class to be used to create {@link RecordWriter} instances that
	 * write to GeoWave.
	 */
	public static class GeoWaveRecordWriter extends
			RecordWriter<GeoWaveOutputKey<Object>, Object>
	{
		private final Map<String, Writer<?>> adapterTypeNameToIndexWriterCache = new HashMap<>();
		private final TransientAdapterStore adapterStore;
		private final IndexStore indexStore;
		private final DataStore dataStore;

		public GeoWaveRecordWriter(
				final TaskAttemptContext context,
				final DataStore dataStore,
				final IndexStore indexStore,
				final TransientAdapterStore adapterStore ) {
			this.dataStore = dataStore;
			this.adapterStore = adapterStore;
			this.indexStore = indexStore;
		}

		/**
		 * Push a mutation into a table. If table is null, the defaultTable will
		 * be used. If canCreateTable is set, the table will be created if it
		 * does not exist. The table name must only contain alphanumerics and
		 * underscore.
		 */
		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public void write(
				final GeoWaveOutputKey ingestKey,
				final Object data )
				throws IOException {
			boolean success = false;
			String errorMessage = null;

			if (ingestKey.getIndexNames().length == 0) {
				throw new IOException(
						"Empty index name input list");
			}

			final DataTypeAdapter<?> adapter = ingestKey.getAdapter(adapterStore);
			if (adapter != null) {
				final Writer indexWriter = getIndexWriter(
						adapter,
						ingestKey.getIndexNames());
				if (indexWriter != null) {
					final InsertionIds writeList = indexWriter.write(data);

					if (!writeList.isEmpty()) {
						success = true;
					}
					else {
						errorMessage = "Empty write list";
					}
				}
				else {
					errorMessage = "Cannot write to index '" + Arrays.toString(ingestKey.getIndexNames()) + "'";
				}
			}
			else {
				errorMessage = "Adapter '" + ingestKey.getTypeName() + "' does not exist";
			}

			if (!success) {
				throw new IOException(
						errorMessage);
			}
		}

		private synchronized Writer<?> getIndexWriter(
				final DataTypeAdapter<?> adapter,
				final String[] indexNames )
				throws MismatchedIndexToAdapterMapping {
			Writer<?> writer = adapterTypeNameToIndexWriterCache.get(adapter.getTypeName());
			if (writer == null) {
				final Index[] indices = new Index[indexNames.length];
				int i = 0;
				for (final String indexName : indexNames) {
					final Index index = indexStore.getIndex(indexName);
					if (index != null) {
						indices[i++] = index;
					}
					else {
						LOGGER.warn("Index '" + indexName + "' does not exist");
					}
				}
				dataStore.addType(
						adapter,
						indices);
				writer = dataStore.createWriter(adapter.getTypeName());

				adapterTypeNameToIndexWriterCache.put(
						adapter.getTypeName(),
						writer);

			}
			return writer;
		}

		@Override
		public synchronized void close(
				final TaskAttemptContext attempt )
				throws IOException,
				InterruptedException {
			for (final Writer<?> indexWriter : adapterTypeNameToIndexWriterCache.values()) {
				indexWriter.close();
			}
		}
	}

}
