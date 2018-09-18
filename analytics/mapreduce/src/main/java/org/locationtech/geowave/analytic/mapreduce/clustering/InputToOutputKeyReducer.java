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
package org.locationtech.geowave.analytic.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.param.OutputParameters;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.mapreduce.GeoWaveWritableInputReducer;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputFormat;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy data from an GeoWave Input to a index using the same adapter.
 * 
 */

public class InputToOutputKeyReducer extends
		GeoWaveWritableInputReducer<GeoWaveOutputKey, Object>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(InputToOutputKeyReducer.class);

	private GeoWaveOutputKey outputKey;
	private InternalAdapterStore internalAdapterStore;

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		outputKey.setAdapterId(internalAdapterStore.getAdapterId(key.getInternalAdapterId()));
		for (final Object value : values) {
			context.write(
					outputKey,
					value);
		}
	}

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		internalAdapterStore = GeoWaveOutputFormat.getJobContextInternalAdapterStore(context);
		final ScopedJobConfiguration config = new ScopedJobConfiguration(
				context.getConfiguration(),
				InputToOutputKeyReducer.class,
				LOGGER);
		final ByteArrayId indexId = new ByteArrayId(
				config.getString(
						OutputParameters.Output.INDEX_ID,
						"na"));
		final List<ByteArrayId> indexIds = new ArrayList<ByteArrayId>();
		indexIds.add(indexId);
		outputKey = new GeoWaveOutputKey(
				new ByteArrayId(
						"na"),
				indexIds);
	}
}
