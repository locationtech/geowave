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
package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

public interface MapReduceDataStore extends
		DataStore
{

	public RecordReader<GeoWaveInputKey, ?> createRecordReader(
			DistributableQuery query,
			QueryOptions queryOptions,
			AdapterStore adapterStore,
			DataStatisticsStore statsStore,
			IndexStore indexStore,
			boolean isOutputWritable,
			InputSplit inputSplit )
			throws IOException,
			InterruptedException;

	public List<InputSplit> getSplits(
			DistributableQuery query,
			QueryOptions queryOptions,
			AdapterStore adapterStore,
			DataStatisticsStore statsStore,
			IndexStore indexStore,
			Integer minSplits,
			Integer maxSplits )
			throws IOException,
			InterruptedException;
}
