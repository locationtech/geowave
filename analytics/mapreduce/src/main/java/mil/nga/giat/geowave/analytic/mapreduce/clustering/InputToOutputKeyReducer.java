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
package mil.nga.giat.geowave.analytic.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.param.OutputParameters;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;
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

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, Object>.Context context )
			throws IOException,
			InterruptedException {
		outputKey.setAdapterId(key.getAdapterId());
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
