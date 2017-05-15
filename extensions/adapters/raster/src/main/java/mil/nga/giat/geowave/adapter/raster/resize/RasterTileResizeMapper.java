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
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.coverage.grid.GridCoverage;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.mapreduce.GeoWaveWritableOutputMapper;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class RasterTileResizeMapper extends
		GeoWaveWritableOutputMapper<GeoWaveInputKey, GridCoverage>
{
	private RasterTileResizeHelper helper;

	@Override
	protected void mapNativeValue(
			final GeoWaveInputKey key,
			final GridCoverage value,
			final MapContext<GeoWaveInputKey, GridCoverage, GeoWaveInputKey, Object> context )
			throws IOException,
			InterruptedException {
		if (helper.isOriginalCoverage(key.getAdapterId())) {
			final DataAdapter<?> adapter = super.serializationTool.getAdapter(key.getAdapterId());
			if ((adapter != null) && (adapter instanceof RasterDataAdapter)) {
				final Iterator<GridCoverage> coverages = helper.getCoveragesForIndex(value);
				if (coverages == null) {
					LOGGER.error("Couldn't get coverages instance, getCoveragesForIndex returned null");
					throw new IOException(
							"Couldn't get coverages instance, getCoveragesForIndex returned null");
				}
				while (coverages.hasNext()) {
					final GridCoverage c = coverages.next();
					// it should be a FitToIndexGridCoverage because it was just
					// converted above
					if (c instanceof FitToIndexGridCoverage) {
						final GeoWaveInputKey inputKey = new GeoWaveInputKey(
								helper.getNewCoverageId(),
								((FitToIndexGridCoverage) c).getInsertionId());
						inputKey.setInsertionId(((FitToIndexGridCoverage) c).getInsertionId());
						context.write(
								inputKey,
								c);
					}
				}
			}
		}
	}

	@Override
	protected void setup(
			final Mapper<GeoWaveInputKey, GridCoverage, GeoWaveInputKey, ObjectWritable>.Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		helper = new RasterTileResizeHelper(
				context);
	}

}
