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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.opengis.coverage.grid.GridCoverage;

import mil.nga.giat.geowave.adapter.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

public class RasterTileResizeHelper
{
	private RasterDataAdapter oldAdapter;
	private RasterDataAdapter newAdapter;
	private final PrimaryIndex index;
	private final List<ByteArrayId> indexIds;

	public RasterTileResizeHelper(
			final JobContext context ) {
		index = JobContextIndexStore.getIndices(context)[0];
		indexIds = new ArrayList<ByteArrayId>();
		indexIds.add(index.getId());
		final DataAdapter[] adapters = JobContextAdapterStore.getDataAdapters(context);
		final Configuration conf = context.getConfiguration();
		final String oldAdapterId = conf.get(RasterTileResizeJobRunner.OLD_ADAPTER_ID_KEY);
		final String newAdapterId = conf.get(RasterTileResizeJobRunner.NEW_ADAPTER_ID_KEY);
		for (final DataAdapter adapter : adapters) {
			if (adapter.getAdapterId().getString().equals(
					oldAdapterId)) {
				oldAdapter = (RasterDataAdapter) adapter;
			}
			else if (adapter.getAdapterId().getString().equals(
					newAdapterId)) {
				if (((RasterDataAdapter) adapter).getTransform() == null) {
					// the new adapter doesn't have a merge strategy - resizing
					// will require merging, so default to NoDataMergeStrategy
					newAdapter = new RasterDataAdapter(
							(RasterDataAdapter) adapter,
							newAdapterId,
							new NoDataMergeStrategy());
				}
				else {
					newAdapter = (RasterDataAdapter) adapter;
				}
			}
		}
	}

	public GeoWaveOutputKey getGeoWaveOutputKey() {
		return new GeoWaveOutputKey(
				newAdapter.getAdapterId(),
				indexIds);
	}

	public Iterator<GridCoverage> getCoveragesForIndex(
			final GridCoverage existingCoverage ) {
		return newAdapter.convertToIndex(
				index,
				existingCoverage);
	}

	protected GridCoverage getMergedCoverage(
			final GeoWaveInputKey key,
			final Iterable<Object> values )
			throws IOException,
			InterruptedException {
		GridCoverage mergedCoverage = null;
		MergeableRasterTile<?> mergedTile = null;
		boolean needsMerge = false;
		final Iterator it = values.iterator();
		while (it.hasNext()) {
			final Object value = it.next();
			if (value instanceof GridCoverage) {
				if (mergedCoverage == null) {
					mergedCoverage = (GridCoverage) value;
				}
				else {
					if (!needsMerge) {
						mergedTile = newAdapter.getRasterTileFromCoverage(mergedCoverage);
						needsMerge = true;
					}
					final MergeableRasterTile thisTile = newAdapter.getRasterTileFromCoverage((GridCoverage) value);
					if (mergedTile != null) {
						mergedTile.merge(thisTile);
					}
				}
			}
		}
		if (needsMerge) {
			mergedCoverage = newAdapter.getCoverageFromRasterTile(
					mergedTile,
					key.getDataId(),
					index);
		}
		return mergedCoverage;
	}

	public ByteArrayId getNewCoverageId() {
		return newAdapter.getAdapterId();
	}

	public boolean isOriginalCoverage(
			final ByteArrayId adapterId ) {
		return oldAdapter.getAdapterId().equals(
				adapterId);
	}
}
