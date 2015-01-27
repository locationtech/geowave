package mil.nga.giat.geowave.raster.resize;

import java.io.IOException;
import java.util.Iterator;

import mil.nga.giat.geowave.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.accumulo.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.raster.adapter.RasterTile;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.opengis.coverage.grid.GridCoverage;

public class RasterTileResizeHelper
{
	private RasterDataAdapter oldAdapter;
	private RasterDataAdapter newAdapter;
	private final Index index;

	public RasterTileResizeHelper(
			final JobContext context ) {
		index = JobContextIndexStore.getIndices(context)[0];
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
				newAdapter = (RasterDataAdapter) adapter;
			}
		}
	}

	public GeoWaveOutputKey getGeoWaveOutputKey() {
		return new GeoWaveOutputKey(
				newAdapter.getAdapterId(),
				index.getId());
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
		RasterTile<?> mergedTile = null;
		boolean needsMerge = false;
		for (final Object value : values) {
			if (value instanceof GridCoverage) {
				if (mergedCoverage == null) {
					mergedCoverage = (GridCoverage) value;
				}
				else {
					if (!needsMerge) {
						mergedTile = newAdapter.getRasterTileFromCoverage(mergedCoverage);
						needsMerge = true;
					}
					final RasterTile thisTile = newAdapter.getRasterTileFromCoverage((GridCoverage) value);
					mergedTile.merge(thisTile);
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
