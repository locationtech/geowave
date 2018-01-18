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
package mil.nga.giat.geowave.core.ingest.hdfs.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.ingest.DataAdapterProvider;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputFormat;

/**
 * This class can be sub-classed to run map-reduce jobs within the ingest
 * framework using plugins provided by types that are discovered through SPI.
 *
 * @param <T>
 *            The type of map-reduce ingest plugin that can be persisted to the
 *            map-reduce job configuration and used by the mapper and/or reducer
 *            to ingest data
 */
abstract public class AbstractMapReduceIngest<T extends Persistable & DataAdapterProvider<?>> extends
		Configured implements
		Tool
{
	public static final String INGEST_PLUGIN_KEY = "INGEST_PLUGIN";
	public static final String GLOBAL_VISIBILITY_KEY = "GLOBAL_VISIBILITY";
	public static final String PRIMARY_INDEX_IDS_KEY = "PRIMARY_INDEX_IDS";
	private static String JOB_NAME = "%s ingest from %s to namespace %s (%s)";
	protected final DataStorePluginOptions dataStoreOptions;
	protected final List<IndexPluginOptions> indexOptions;
	protected final VisibilityOptions ingestOptions;
	protected final Path inputFile;
	protected final String typeName;
	protected final IngestFromHdfsPlugin<?, ?> parentPlugin;
	protected final T ingestPlugin;

	public AbstractMapReduceIngest(
			final DataStorePluginOptions dataStoreOptions,
			final List<IndexPluginOptions> indexOptions,
			final VisibilityOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin<?, ?> parentPlugin,
			final T ingestPlugin ) {
		this.dataStoreOptions = dataStoreOptions;
		this.indexOptions = indexOptions;
		this.ingestOptions = ingestOptions;
		this.inputFile = inputFile;
		this.typeName = typeName;
		this.parentPlugin = parentPlugin;
		this.ingestPlugin = ingestPlugin;
	}

	public String getJobName() {
		return String.format(
				JOB_NAME,
				typeName,
				inputFile.toString(),
				dataStoreOptions.getGeowaveNamespace(),
				getIngestDescription());
	}

	abstract protected String getIngestDescription();

	protected static List<ByteArrayId> getPrimaryIndexIds(
			final Configuration conf ) {
		final String primaryIndexIdStr = conf.get(AbstractMapReduceIngest.PRIMARY_INDEX_IDS_KEY);
		final List<ByteArrayId> primaryIndexIds = new ArrayList<ByteArrayId>();
		if ((primaryIndexIdStr != null) && !primaryIndexIdStr.isEmpty()) {
			final String[] indexIds = primaryIndexIdStr.split(",");
			for (final String indexId : indexIds) {
				primaryIndexIds.add(new ByteArrayId(
						indexId));
			}
		}
		return primaryIndexIds;
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {
		final Configuration conf = getConf();
		conf.set(
				INGEST_PLUGIN_KEY,
				ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(ingestPlugin)));
		if (ingestOptions.getVisibility() != null) {
			conf.set(
					GLOBAL_VISIBILITY_KEY,
					ingestOptions.getVisibility());
		}
		final Job job = new Job(
				conf,
				getJobName());
		final StringBuilder indexIds = new StringBuilder();
		final List<PrimaryIndex> indexes = new ArrayList<PrimaryIndex>();
		for (final IndexPluginOptions indexOption : indexOptions) {
			final PrimaryIndex primaryIndex = indexOption.createPrimaryIndex();
			indexes.add(primaryIndex);
			if (primaryIndex != null) {
				// add index
				GeoWaveOutputFormat.addIndex(
						job.getConfiguration(),
						primaryIndex);
				if (indexIds.length() != 0) {
					indexIds.append(",");
				}
				indexIds.append(primaryIndex.getId().getString());
			}
		}

		job.getConfiguration().set(
				PRIMARY_INDEX_IDS_KEY,
				indexIds.toString());

		job.setJarByClass(AbstractMapReduceIngest.class);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(
				job,
				parentPlugin.getAvroSchema());
		FileInputFormat.setInputPaths(
				job,
				inputFile);

		setupMapper(job);
		setupReducer(job);
		// set geowave output format
		job.setOutputFormatClass(GeoWaveOutputFormat.class);

		GeoWaveOutputFormat.setStoreOptions(
				job.getConfiguration(),
				dataStoreOptions);
		PrimaryIndex[] indexesArray = new PrimaryIndex[indexes.size()];
		final WritableDataAdapter<?>[] dataAdapters = ingestPlugin.getDataAdapters(ingestOptions.getVisibility());
		for (final WritableDataAdapter<?> dataAdapter : dataAdapters) {
			dataAdapter.init(indexes.toArray(indexesArray));
			GeoWaveOutputFormat.addDataAdapter(
					job.getConfiguration(),
					dataAdapter);
		}

		job.setSpeculativeExecution(false);

		// add required indices
		final PrimaryIndex[] requiredIndices = parentPlugin.getRequiredIndices();
		if (requiredIndices != null) {
			for (final PrimaryIndex requiredIndex : requiredIndices) {
				GeoWaveOutputFormat.addIndex(
						job.getConfiguration(),
						requiredIndex);
			}
		}
		return job.waitForCompletion(true) ? 0 : -1;
	}

	abstract protected void setupMapper(
			Job job );

	abstract protected void setupReducer(
			Job job );
}
