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
package org.locationtech.geowave.adapter.raster.resize;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.operations.ResizeCommand;
import org.locationtech.geowave.adapter.raster.operations.options.RasterTileResizeCommandLineOptions;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextInternalAdapterStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RasterTileResizeJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RasterTileResizeJobRunner.class);

	public static final String NEW_TYPE_NAME_KEY = "NEW_TYPE_NAME";
	public static final String NEW_ADAPTER_ID_KEY = "NEW_ADAPTER_ID";
	public static final String OLD_TYPE_NAME_KEY = "OLD_TYPE_NAME";
	public static final String OLD_ADAPTER_ID_KEY = "OLD_ADAPTER_ID";

	private final DataStorePluginOptions inputStoreOptions;
	private final DataStorePluginOptions outputStoreOptions;
	protected RasterTileResizeCommandLineOptions rasterResizeOptions;

	public RasterTileResizeJobRunner(
			final DataStorePluginOptions inputStoreOptions,
			final DataStorePluginOptions outputStoreOptions,
			final RasterTileResizeCommandLineOptions rasterResizeOptions ) {
		this.inputStoreOptions = inputStoreOptions;
		this.outputStoreOptions = outputStoreOptions;
		this.rasterResizeOptions = rasterResizeOptions;
	}

	/**
	 * Main method to execute the MapReduce analytic.
	 */
	public int runJob()
			throws IOException,
			InterruptedException,
			ClassNotFoundException {
		Configuration conf = super.getConf();
		if (conf == null) {
			conf = new Configuration();
			setConf(conf);
		}
		GeoWaveConfiguratorBase.setRemoteInvocationParams(
				rasterResizeOptions.getHdfsHostPort(),
				rasterResizeOptions.getJobTrackerOrResourceManHostPort(),
				conf);
		conf.set(
				OLD_TYPE_NAME_KEY,
				rasterResizeOptions.getInputCoverageName());
		conf.set(
				NEW_TYPE_NAME_KEY,
				rasterResizeOptions.getOutputCoverageName());
		final Job job = new Job(
				conf);

		job.setJarByClass(this.getClass());

		job.setJobName("Converting " + rasterResizeOptions.getInputCoverageName() + " to tile size="
				+ rasterResizeOptions.getOutputTileSize());

		job.setMapperClass(RasterTileResizeMapper.class);
		job.setCombinerClass(RasterTileResizeCombiner.class);
		job.setReducerClass(RasterTileResizeReducer.class);
		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(GridCoverage.class);
		job.setNumReduceTasks(8);

		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				rasterResizeOptions.getMinSplits());
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				rasterResizeOptions.getMaxSplits());

		GeoWaveInputFormat.setStoreOptions(
				job.getConfiguration(),
				inputStoreOptions);

		final InternalAdapterStore internalAdapterStore = inputStoreOptions.createInternalAdapterStore();
		final short internalAdapterId = internalAdapterStore.getAdapterId(rasterResizeOptions.getInputCoverageName());
		final DataTypeAdapter adapter = inputStoreOptions.createAdapterStore().getAdapter(
				internalAdapterId).getAdapter();

		if (adapter == null) {
			throw new IllegalArgumentException(
					"Adapter for coverage '" + rasterResizeOptions.getInputCoverageName()
							+ "' does not exist in namespace '" + inputStoreOptions.getGeowaveNamespace() + "'");
		}

		final RasterDataAdapter newAdapter = new RasterDataAdapter(
				(RasterDataAdapter) adapter,
				rasterResizeOptions.getOutputCoverageName(),
				rasterResizeOptions.getOutputTileSize());

		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				adapter);
		JobContextAdapterStore.addDataAdapter(
				job.getConfiguration(),
				newAdapter);
		Index index = null;
		final IndexStore indexStore = inputStoreOptions.createIndexStore();
		if (rasterResizeOptions.getIndexName() != null) {
			index = indexStore.getIndex(rasterResizeOptions.getIndexName());
		}
		if (index == null) {
			try (CloseableIterator<Index> indices = indexStore.getIndices()) {
				index = indices.next();
			}
			if (index == null) {
				throw new IllegalArgumentException(
						"Index does not exist in namespace '" + inputStoreOptions.getGeowaveNamespace() + "'");
			}
		}
		GeoWaveOutputFormat.setStoreOptions(
				job.getConfiguration(),
				outputStoreOptions);
		GeoWaveOutputFormat.addIndex(
				job.getConfiguration(),
				index);
		final DataStore store = outputStoreOptions.createDataStore();
		store.addType(
				newAdapter,
				index);
		final short newInternalAdapterId = outputStoreOptions.createInternalAdapterStore().addTypeName(
				newAdapter.getTypeName());
		// what if the adapter IDs are the same, but the internal IDs are
		// different (unlikely corner case, but seemingly possible)
		JobContextInternalAdapterStore.addTypeName(
				job.getConfiguration(),
				newAdapter.getTypeName(),
				newInternalAdapterId);
		JobContextInternalAdapterStore.addTypeName(
				job.getConfiguration(),
				adapter.getTypeName(),
				internalAdapterId);

		job.getConfiguration().setInt(
				OLD_ADAPTER_ID_KEY,
				internalAdapterId);

		job.getConfiguration().setInt(
				NEW_ADAPTER_ID_KEY,
				newInternalAdapterId);
		if (outputStoreOptions.getFactoryOptions().getStoreOptions().isPersistDataStatistics()) {
			try {
				// this is done primarily to ensure stats merging is enabled
				// before the
				// distributed ingest
				outputStoreOptions.createDataStoreOperations().createMetadataWriter(
						MetadataType.STATS).close();
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to create stats writer",
						e);
			}
		}
		boolean retVal = false;
		try {
			retVal = job.waitForCompletion(true);
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Error waiting for map reduce tile resize job: ",
					ex);
		}

		final CloseableIterator<Object> obj = outputStoreOptions.createDataStore().query(
				QueryBuilder.newBuilder().addTypeName(
						rasterResizeOptions.getOutputCoverageName()).indexName(
						index.getName()).build());
		while (obj.hasNext()) {
			obj.next();
		}

		return retVal ? 0 : 1;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final ConfigOptions opts = new ConfigOptions();
		final OperationParser parser = new OperationParser();
		parser.addAdditionalObject(opts);
		final ResizeCommand command = new ResizeCommand();
		final CommandLineOperationParams params = parser.parse(
				command,
				args);
		opts.prepare(params);
		final int res = ToolRunner.run(
				new Configuration(),
				command.createRunner(params),
				args);
		System.exit(res);
	}

	@Override
	public int run(
			final String[] args )
			throws Exception {

		// parse args to find command line etc...

		return runJob();
	}

}
