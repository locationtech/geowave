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
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.mapreduce.GeoWaveWritableInputReducer;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * A basic implementation of copy as a reducer
 */
public class StoreCopyReducer extends
		GeoWaveWritableInputReducer<GeoWaveOutputKey, Object>
{
	private AdapterIndexMappingStore store;
	private InternalAdapterStore internalAdapterStore;

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		store = GeoWaveOutputFormat.getJobContextAdapterIndexMappingStore(context);
		internalAdapterStore = GeoWaveOutputFormat.getJobContextInternalAdapterStore(context);
	}

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		final Iterator<Object> objects = values.iterator();
		while (objects.hasNext()) {
			final AdapterToIndexMapping mapping = store.getIndicesForAdapter(key.getInternalAdapterId());
			context.write(
					new GeoWaveOutputKey<>(
							internalAdapterStore.getTypeName(mapping.getAdapterId()),
							mapping.getIndexNames()),
					objects.next());
		}
	}

}
