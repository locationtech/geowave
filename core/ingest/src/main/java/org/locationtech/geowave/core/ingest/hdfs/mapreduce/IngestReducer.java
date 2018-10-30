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

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * This is the map-reduce reducer for ingestion with both the mapper and
 * reducer.
 */
public class IngestReducer extends
		Reducer<WritableComparable<?>, Writable, GeoWaveOutputKey, Object>
{
	private IngestWithReducer ingestWithReducer;
	private String globalVisibility;
	private String[] indexNames;

	@Override
	protected void reduce(
			final WritableComparable<?> key,
			final Iterable<Writable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		try (CloseableIterator<GeoWaveData> data = ingestWithReducer.toGeoWaveData(
				key,
				indexNames,
				globalVisibility,
				values)) {
			while (data.hasNext()) {
				final GeoWaveData d = data.next();
				context.write(
						new GeoWaveOutputKey<>(
								d),
						d.getValue());
			}
		}
	}

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		try {
			final String ingestWithReducerStr = context.getConfiguration().get(
					AbstractMapReduceIngest.INGEST_PLUGIN_KEY);
			final byte[] ingestWithReducerBytes = ByteArrayUtils.byteArrayFromString(ingestWithReducerStr);
			ingestWithReducer = (IngestWithReducer) PersistenceUtils.fromBinary(ingestWithReducerBytes);
			globalVisibility = context.getConfiguration().get(
					AbstractMapReduceIngest.GLOBAL_VISIBILITY_KEY);
			indexNames = AbstractMapReduceIngest.getIndexNames(context.getConfiguration());
		}
		catch (final Exception e) {
			throw new IllegalArgumentException(
					e);
		}
	}
}
