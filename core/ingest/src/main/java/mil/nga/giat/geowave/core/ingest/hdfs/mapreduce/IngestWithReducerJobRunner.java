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

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.core.store.util.GenericTypeResolver;

/**
 * This class will run the ingestion process by using a mapper to aggregate key
 * value pairs and a reducer to ingest data into GeoWave.
 */
public class IngestWithReducerJobRunner extends
		AbstractMapReduceIngest<IngestWithReducer<?, ?, ?, ?>>
{
	public IngestWithReducerJobRunner(
			final DataStorePluginOptions storeOptions,
			final List<IndexPluginOptions> indexOptions,
			final VisibilityOptions ingestOptions,
			final Path inputFile,
			final String typeName,
			final IngestFromHdfsPlugin parentPlugin,
			final IngestWithReducer ingestPlugin ) {
		super(
				storeOptions,
				indexOptions,
				ingestOptions,
				inputFile,
				typeName,
				parentPlugin,
				ingestPlugin);
	}

	@Override
	protected String getIngestDescription() {
		return "with reducer";
	}

	@Override
	protected void setupMapper(
			final Job job ) {
		job.setMapperClass(IntermediateKeyValueMapper.class);
		final Class<?>[] genericClasses = GenericTypeResolver.resolveTypeArguments(
				ingestPlugin.getClass(),
				IngestWithReducer.class);
		// set mapper output info
		job.setMapOutputKeyClass(genericClasses[1]);
		job.setMapOutputValueClass(genericClasses[2]);
	}

	@Override
	protected void setupReducer(
			final Job job ) {
		job.setReducerClass(IngestReducer.class);
		if (job.getNumReduceTasks() <= 1) {
			// the default is one reducer, if its only one, set it to 8 as the
			// default
			job.setNumReduceTasks(8);
		}
	}

}
