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
package mil.nga.giat.geowave.mapreduce.dedupe;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A basic implementation of deduplication as a reducer
 */
public class GeoWaveDedupeReducer extends
		Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>
{

	@Override
	protected void reduce(
			final GeoWaveInputKey key,
			final Iterable<ObjectWritable> values,
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		final Iterator<ObjectWritable> objects = values.iterator();
		if (objects.hasNext()) {
			context.write(
					key,
					objects.next());
		}
	}

}
