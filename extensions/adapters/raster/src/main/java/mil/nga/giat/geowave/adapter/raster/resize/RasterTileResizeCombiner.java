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
package mil.nga.giat.geowave.adapter.raster.resize;

import java.io.IOException;

import mil.nga.giat.geowave.mapreduce.GeoWaveReducer;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeCombiner extends
		GeoWaveReducer
{
	private RasterTileResizeHelper helper;

	@Override
	protected void reduceNativeValues(
			final GeoWaveInputKey key,
			final Iterable<Object> values,
			final ReduceContext<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException {
		final GridCoverage mergedCoverage = helper.getMergedCoverage(
				key,
				values);
		if (mergedCoverage != null) {
			context.write(
					key,
					mergedCoverage);
		}

	}

	@Override
	protected void setup(
			final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
