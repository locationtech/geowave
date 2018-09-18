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
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat.GeoWaveRecordWriter;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRecordReader;
import org.locationtech.geowave.mapreduce.splits.SplitsProvider;

public class BaseMapReduceDataStore extends
		BaseDataStore implements
		MapReduceDataStore
{
	protected final SplitsProvider splitsProvider;

	public BaseMapReduceDataStore(
			final IndexStore indexStore,
			final PersistentAdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final MapReduceDataStoreOperations operations,
			final DataStoreOptions options,
			final InternalAdapterStore adapterMappingStore ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options,
				adapterMappingStore);
		splitsProvider = createSplitsProvider();
	}

	@Override
	public RecordWriter<GeoWaveOutputKey<Object>, Object> createRecordWriter(
			final TaskAttemptContext context,
			final IndexStore jobContextIndexStore,
			final TransientAdapterStore jobContextAdapterStore ) {
		return new GeoWaveRecordWriter(
				context,
				this,
				jobContextIndexStore,
				jobContextAdapterStore);
	}

	@Override
	public void prepareRecordWriter(
			final Configuration conf ) {
		// generally this can be a no-op, but gives the datastore an opportunity
		// to set specialized configuration for a job prior to submission
	}

	@Override
	public RecordReader<GeoWaveInputKey, ?> createRecordReader(
			final CommonQueryOptions commonOptions,
			final DataTypeQueryOptions<?> typeOptions,
			final IndexQueryOptions indexOptions,
			final QueryConstraints constraints,
			final TransientAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore aimStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final boolean isOutputWritable,
			final InputSplit inputSplit )
			throws IOException,
			InterruptedException {
		return new GeoWaveRecordReader(
				commonOptions,
				typeOptions,
				indexOptions,
				constraints,
				isOutputWritable,
				adapterStore,
				internalAdapterStore,
				aimStore,
				indexStore,
				(MapReduceDataStoreOperations) baseOperations);
	}

	protected SplitsProvider createSplitsProvider() {
		return new SplitsProvider();
	}

	@Override
	public List<InputSplit> getSplits(
			final CommonQueryOptions commonOptions,
			final DataTypeQueryOptions<?> typeOptions,
			final IndexQueryOptions indexOptions,
			final QueryConstraints constraints,
			final TransientAdapterStore adapterStore,
			final AdapterIndexMappingStore aimStore,
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore,
			final IndexStore indexStore,
			final JobContext context,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		return splitsProvider.getSplits(
				baseOperations,
				commonOptions,
				typeOptions,
				indexOptions,
				constraints,
				adapterStore,
				statsStore,
				internalAdapterStore,
				indexStore,
				indexMappingStore,
				minSplits,
				maxSplits);
	}
}
