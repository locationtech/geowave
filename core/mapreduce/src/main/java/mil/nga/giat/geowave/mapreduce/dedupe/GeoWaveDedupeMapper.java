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

import mil.nga.giat.geowave.mapreduce.GeoWaveWritableOutputMapper;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.mapreduce.MapContext;

/**
 * Basically an identity mapper used for the deduplication job
 */
public class GeoWaveDedupeMapper extends
		GeoWaveWritableOutputMapper<GeoWaveInputKey, Object>
{

	@Override
	protected void mapNativeValue(
			final GeoWaveInputKey key,
			final Object value,
			final MapContext<GeoWaveInputKey, Object, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				value);
	}

}
