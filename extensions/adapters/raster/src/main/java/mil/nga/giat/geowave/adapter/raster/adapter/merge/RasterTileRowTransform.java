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
package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

/**
 * This class can be used by both the RasterTileCombiner and the
 * RasterTileVisibilityCombiner to execute the merge strategy
 */
public class RasterTileRowTransform<T extends Persistable> implements
		RowTransform<Mergeable>
{
	public static final String TRANSFORM_NAME = "RasterTile";
	public static final String MERGE_STRATEGY_KEY = "MERGE_STRATEGY";
	private RootMergeStrategy<T> mergeStrategy;
	// this priority is fairly arbitrary at the moment
	private static final int RASTER_TILE_PRIORITY = 4;

	public Mergeable transform(
			final ByteArrayId adapterId,
			final Mergeable mergeable ) {
		if ((mergeable != null) && (mergeable instanceof RasterTile)) {
			final RasterTile<T> rasterTile = (RasterTile) mergeable;
			return new MergeableRasterTile<T>(
					rasterTile.getDataBuffer(),
					rasterTile.getMetadata(),
					mergeStrategy,
					adapterId);
		}
		return mergeable;
	}

	@Override
	public void initOptions(
			final Map<String, String> options )
			throws IOException {
		final String mergeStrategyStr = options.get(MERGE_STRATEGY_KEY);
		if (mergeStrategyStr != null) {
			final byte[] mergeStrategyBytes = ByteArrayUtils.byteArrayFromString(mergeStrategyStr);
			mergeStrategy = (RootMergeStrategy<T>) PersistenceUtils.fromBinary(mergeStrategyBytes);
		}
	}

	@Override
	public Mergeable getRowAsMergeableObject(
			final ByteArrayId adapterId,
			final ByteArrayId fieldId,
			final byte[] rowValueBinary ) {
		final RasterTile mergeable = new RasterTile();

		if (mergeable != null) {
			mergeable.fromBinary(rowValueBinary);
		}
		return transform(
				adapterId,
				mergeable);
	}

	@Override
	public byte[] getBinaryFromMergedObject(
			final Mergeable rowObject ) {
		return rowObject.toBinary();
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public String getTransformName() {
		return TRANSFORM_NAME;
	}

	@Override
	public int getBaseTransformPriority() {
		return RASTER_TILE_PRIORITY;
	}
}
