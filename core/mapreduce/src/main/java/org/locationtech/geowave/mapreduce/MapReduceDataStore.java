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
package org.locationtech.geowave.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;

public interface MapReduceDataStore extends
		DataStore
{

	public RecordReader<GeoWaveInputKey, ?> createRecordReader(
			CommonQueryOptions commonOptions,
			DataTypeQueryOptions<?> typeOptions,
			IndexQueryOptions indexOptions,
			QueryConstraints constraints,
			TransientAdapterStore adapterStore,
			InternalAdapterStore internalAdapterStore,
			AdapterIndexMappingStore aimStore,
			DataStatisticsStore statsStore,
			IndexStore indexStore,
			boolean isOutputWritable,
			InputSplit inputSplit )
			throws IOException,
			InterruptedException;

	public List<InputSplit> getSplits(
			CommonQueryOptions commonOptions,
			DataTypeQueryOptions<?> typeOptions,
			IndexQueryOptions indexOptions,
			QueryConstraints constraints,
			TransientAdapterStore adapterStore,
			AdapterIndexMappingStore aimStore,
			DataStatisticsStore statsStore,
			InternalAdapterStore internalAdapterStore,
			IndexStore indexStore,
			JobContext context,
			Integer minSplits,
			Integer maxSplits )
			throws IOException,
			InterruptedException;

	public RecordWriter<GeoWaveOutputKey<Object>, Object> createRecordWriter(
			TaskAttemptContext context,
			IndexStore jobContextIndexStore,
			TransientAdapterStore jobContextAdapterStore );

	public void prepareRecordWriter(
			Configuration conf );
}
