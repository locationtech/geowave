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
package org.locationtech.geowave.mapreduce.copy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.JobContextAdapterIndexMappingStore;
import org.locationtech.geowave.mapreduce.JobContextInternalAdapterStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.operations.CopyCommand;
import org.locationtech.geowave.mapreduce.operations.CopyCommandOptions;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;

public class StoreCopyJobRunner extends
		Configured implements
		Tool
{
	private static final Logger LOGGER = Logger.getLogger(StoreCopyJobRunner.class);

	private final DataStorePluginOptions inputStoreOptions;
	private final DataStorePluginOptions outputStoreOptions;
	private final CopyCommandOptions options;
	private final String jobName;

	public StoreCopyJobRunner(
			final DataStorePluginOptions inputStoreOptions,
			final DataStorePluginOptions outputStoreOptions,
			final CopyCommandOptions options,
			final String jobName ) {
		this.inputStoreOptions = inputStoreOptions;
		this.outputStoreOptions = outputStoreOptions;
		this.options = options;
		this.jobName = jobName;
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
				options.getHdfsHostPort(),
				options.getJobTrackerOrResourceManHostPort(),
				conf);

		final Job job = Job.getInstance(conf);

		job.setJarByClass(this.getClass());

		job.setJobName(jobName);

		job.setMapperClass(StoreCopyMapper.class);
		job.setReducerClass(StoreCopyReducer.class);

		job.setInputFormatClass(GeoWaveInputFormat.class);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);

		job.setMapOutputKeyClass(GeoWaveInputKey.class);
		job.setMapOutputValueClass(ObjectWritable.class);
		job.setOutputKeyClass(GeoWaveOutputKey.class);
		job.setOutputValueClass(Object.class);
		job.setNumReduceTasks(options.getNumReducers());

		GeoWaveInputFormat.setMinimumSplitCount(
				job.getConfiguration(),
				options.getMinSplits());
		GeoWaveInputFormat.setMaximumSplitCount(
				job.getConfiguration(),
				options.getMaxSplits());

		GeoWaveInputFormat.setStoreOptions(
				job.getConfiguration(),
				inputStoreOptions);

		GeoWaveOutputFormat.setStoreOptions(
				job.getConfiguration(),
				outputStoreOptions);

		final AdapterIndexMappingStore adapterIndexMappingStore = inputStoreOptions.createAdapterIndexMappingStore();
		try (CloseableIterator<InternalDataAdapter<?>> adapterIt = inputStoreOptions.createAdapterStore().getAdapters()) {
			while (adapterIt.hasNext()) {
				final InternalDataAdapter<?> dataAdapter = adapterIt.next();

				LOGGER.debug("Adding adapter to output config: " + dataAdapter.getTypeName());

				GeoWaveOutputFormat.addDataAdapter(
						job.getConfiguration(),
						dataAdapter);

				final AdapterToIndexMapping mapping = adapterIndexMappingStore.getIndicesForAdapter(dataAdapter
						.getAdapterId());

				JobContextAdapterIndexMappingStore.addAdapterToIndexMapping(
						job.getConfiguration(),
						mapping);
				JobContextInternalAdapterStore.addTypeName(
						job.getConfiguration(),
						dataAdapter.getTypeName(),
						dataAdapter.getAdapterId());
			}
		}

		try (CloseableIterator<Index> indexIt = inputStoreOptions.createIndexStore().getIndices()) {
			while (indexIt.hasNext()) {
				final Index index = indexIt.next();

				LOGGER.debug("Adding index to output config: " + (index.getName()));

				GeoWaveOutputFormat.addIndex(
						job.getConfiguration(),
						index);
			}
		}

		boolean retVal = false;
		try {
			retVal = job.waitForCompletion(true);
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Error waiting for store copy job: ",
					ex);
		}

		return retVal ? 0 : 1;
	}

	public static void main(
			final String[] args )
			throws Exception {
		final ConfigOptions opts = new ConfigOptions();
		final OperationParser parser = new OperationParser();
		parser.addAdditionalObject(opts);
		final CopyCommand command = new CopyCommand();
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
