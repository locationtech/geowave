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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.adapter.raster.adapter.ClientMergeableRasterTile;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;
import org.locationtech.geowave.core.index.ByteArray;
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
	private final short oldAdapterId;
	private final short newAdapterId;
	private final Index index;
	private final String[] indexNames;

	public RasterTileResizeHelper(
			final JobContext context ) {
		index = JobContextIndexStore.getIndices(context)[0];
		indexNames = new String[] {
			index.getName()
		};
		final DataTypeAdapter[] adapters = JobContextAdapterStore.getDataAdapters(context);
		final Configuration conf = context.getConfiguration();
		final String newTypeName = conf.get(RasterTileResizeJobRunner.NEW_TYPE_NAME_KEY);
		oldAdapterId = (short) conf.getInt(
				RasterTileResizeJobRunner.OLD_ADAPTER_ID_KEY,
				-1);
		newAdapterId = (short) conf.getInt(
				RasterTileResizeJobRunner.NEW_ADAPTER_ID_KEY,
				InternalAdapterStoreImpl.getInitialAdapterId(newTypeName));
		for (final DataTypeAdapter adapter : adapters) {
			if (adapter.getTypeName().equals(
					newTypeName)) {
				if (((RasterDataAdapter) adapter).getTransform() == null) {
					// the new adapter doesn't have a merge strategy - resizing
					// will require merging, so default to NoDataMergeStrategy
					newAdapter = new RasterDataAdapter(
							(RasterDataAdapter) adapter,
							newTypeName,
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
				newAdapter.getTypeName(),
				indexNames);
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
					pair == null ? null : new ByteArray(
							pair.getLeft()),
					pair == null ? null : new ByteArray(
							pair.getRight()),
					index);
		}
		return mergedCoverage;
	}

	public short getNewAdapterId() {
		return newAdapterId;
	}

	public ByteArray getNewDataId(
			final GridCoverage coverage ) {
		return newAdapter.getDataId(coverage);
	}

	public String getIndexName() {
		return index.getName();
	}

	public boolean isOriginalCoverage(
			final short adapterId ) {
		return oldAdapterId == adapterId;
	}
}
