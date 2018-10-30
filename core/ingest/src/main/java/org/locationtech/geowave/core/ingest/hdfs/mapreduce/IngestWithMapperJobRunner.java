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
package org.locationtech.geowave.core.ingest.hdfs.mapreduce;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.VisibilityOptions;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * This will run the mapper only ingest process.
 */
public class IngestWithMapperJobRunner extends
		AbstractMapReduceIngest<IngestWithMapper<?, ?>>
{

	public IngestWithMapperJobRunner(
			final DataStorePluginOptions storeOptions,
			final List<IndexPluginOptions> indexOptions,
			final VisibilityOptions ingestOptions,
			final Path inputFile,
			final String formatPluginName,
			final IngestFromHdfsPlugin<?, ?> plugin,
			final IngestWithMapper<?, ?> mapperIngest ) {
		super(
				storeOptions,
				indexOptions,
				ingestOptions,
				inputFile,
				formatPluginName,
				plugin,
				mapperIngest);
	}

	@Override
	protected void setupReducer(
			final Job job ) {
		job.setNumReduceTasks(0);
	}

	@Override
	protected String getIngestDescription() {
		return "map only";
	}

	@Override
	protected void setupMapper(
			final Job job ) {
		job.setMapperClass(IngestMapper.class);
		// set mapper output info
		job.setMapOutputKeyClass(GeoWaveOutputKey.class);
		job.setMapOutputValueClass(Object.class);
	}

}
