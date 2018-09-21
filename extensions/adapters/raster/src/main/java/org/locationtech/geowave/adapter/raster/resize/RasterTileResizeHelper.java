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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.adapter.raster.adapter.ClientMergeableRasterTile;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.mapreduce.JobContextAdapterStore;
import org.locationtech.geowave.mapreduce.JobContextIndexStore;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.mapreduce.output.GeoWaveOutputKey;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeHelper
{
	private RasterDataAdapter newAdapter;
	private final short oldInternalAdapterId;
	private final short newInternalAdapterId;
	private final Index index;
	private final List<ByteArrayId> indexIds;

	public RasterTileResizeHelper(
			final JobContext context ) {
		index = JobContextIndexStore.getIndices(context)[0];
		indexIds = new ArrayList<ByteArrayId>();
		indexIds.add(index.getId());
		final DataTypeAdapter[] adapters = JobContextAdapterStore.getDataAdapters(context);
		final Configuration conf = context.getConfiguration();
		final String newAdapterId = conf.get(RasterTileResizeJobRunner.NEW_ADAPTER_ID_KEY);
		oldInternalAdapterId = (short) conf.getInt(
				RasterTileResizeJobRunner.OLD_INTERNAL_ADAPTER_ID_KEY,
				-1);
		newInternalAdapterId = (short) conf.getInt(
				RasterTileResizeJobRunner.NEW_INTERNAL_ADAPTER_ID_KEY,
				InternalAdapterStoreImpl.getInitialInternalAdapterId(new ByteArrayId(
						newAdapterId)));
		for (final DataTypeAdapter adapter : adapters) {
			if (adapter.getAdapterId().getString().equals(
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
		ClientMergeableRasterTile<?> mergedTile = null;
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
					final ClientMergeableRasterTile thisTile = newAdapter
							.getRasterTileFromCoverage((GridCoverage) value);
					if (mergedTile != null) {
						mergedTile.merge(thisTile);
					}
				}
			}
		}
		if (needsMerge) {
			final Pair<byte[], byte[]> pair = key.getPartitionAndSortKey(index);
			mergedCoverage = newAdapter.getCoverageFromRasterTile(
					mergedTile,
					pair == null ? null : new ByteArrayId(
							pair.getLeft()),
					pair == null ? null : new ByteArrayId(
							pair.getRight()),
					index);
		}
		return mergedCoverage;
	}

	public short getNewInternalAdapterId() {
		return newInternalAdapterId;
	}

	public ByteArrayId getNewDataId(
			final GridCoverage coverage ) {
		return newAdapter.getDataId(coverage);
	}

	public ByteArrayId getIndexId() {
		return index.getId();
	}

	public boolean isOriginalCoverage(
			final short internalAdapterId ) {
		return oldInternalAdapterId == internalAdapterId;
	}
}
