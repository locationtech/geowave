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

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable.It is a
 * reducer that converts to writable objects for both inputs and outputs. This
 * conversion will only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveReducer extends
		Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveReducer.class);
	protected HadoopWritableSerializationTool serializationTool;

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		reduceWritableValues(
				key,
				values,
				context);
	}

	protected void reduceWritableValues(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		final HadoopWritableSerializer<?, Writable> serializer = serializationTool
				.getHadoopWritableSerializerForAdapter(key.getInternalAdapterId());
		final Iterable<Object> transformedValues = Iterables.transform(
				values,
				new Function<ObjectWritable, Object>() {
					@Override
					public Object apply(
							final ObjectWritable writable ) {
						final Object innerObj = writable.get();
						return innerObj instanceof Writable ? serializer.fromWritable((Writable) innerObj) : innerObj;
					}
				});
		reduceNativeValues(
				key,
				transformedValues,
				new NativeReduceContext(
						context,
						serializationTool));
	}

	protected abstract void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final ReduceContext<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		serializationTool = new HadoopWritableSerializationTool(
				context);
	}
}
