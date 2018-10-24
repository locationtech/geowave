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
package org.locationtech.geowave.adapter.raster.resize;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.mapreduce.GeoWaveWritableOutputMapper;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.coverage.grid.GridCoverage;

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
		if (helper.isOriginalCoverage(key.getInternalAdapterId())) {
			final InternalDataAdapter<?> adapter = super.serializationTool.getInternalAdapter(key
					.getInternalAdapterId());
			if ((adapter != null) && (adapter.getAdapter() != null)
					&& (adapter.getAdapter() instanceof RasterDataAdapter)) {
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
						final ByteArray partitionKey = ((FitToIndexGridCoverage) c).getPartitionKey();
						final ByteArray sortKey = ((FitToIndexGridCoverage) c).getSortKey();
						final GeoWaveKey geowaveKey = new GeoWaveKeyImpl(
								helper.getNewDataId(
										c).getBytes(),
								key.getInternalAdapterId(),
								partitionKey == null ? null : partitionKey.getBytes(),
								sortKey == null ? null : sortKey.getBytes(),
								0);
						final GeoWaveInputKey inputKey = new GeoWaveInputKey(
								helper.getNewAdapterId(),
								geowaveKey,
								helper.getIndexName());
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
