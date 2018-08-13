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

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class can be extended by GeoWave analytics. It handles the
 * conversion of native GeoWave objects into objects that are writable. It is a
 * mapper that converts to writable objects for the input. This conversion will
 * only work if the data adapter implements HadoopDataAdapter.
 */
public abstract class GeoWaveWritableInputMapper<KEYOUT, VALUEOUT> extends
		Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveWritableInputMapper.class);
	protected HadoopWritableSerializationTool serializationTool;

	@Override
	protected void map(
			final GeoWaveInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		mapWritableValue(
				key,
				value,
				context);
	}

	protected void mapWritableValue(
			final GeoWaveInputKey key,
			final ObjectWritable value,
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		mapNativeValue(
				key,
				serializationTool.fromWritable(
						key.getInternalAdapterId(),
						value),
				context);

	}

	/**
	 * Helper method to create an object writable from a value managed by the
	 * adapter.
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	protected ObjectWritable toWritableValue(
			final GeoWaveInputKey key,
			final Object value ) {
		return serializationTool.toWritable(
				key.getInternalAdapterId(),
				value);
	}

	protected abstract void mapNativeValue(
			final GeoWaveInputKey key,
			final Object value,
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException;

	@Override
	protected void setup(
			final Mapper<GeoWaveInputKey, ObjectWritable, KEYOUT, VALUEOUT>.Context context )
			throws IOException,
			InterruptedException {
		serializationTool = new HadoopWritableSerializationTool(
				context);
	}
}
