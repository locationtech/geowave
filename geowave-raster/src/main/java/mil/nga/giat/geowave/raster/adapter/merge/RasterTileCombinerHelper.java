package mil.nga.giat.geowave.raster.adapter.merge;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.raster.adapter.RasterTile;

import org.apache.accumulo.core.data.Key;

/**
 * This class can be used by both the RasterTileCombiner and the
 * RasterTileVisibilityCombiner to execute the merge strategy
 */
public class RasterTileCombinerHelper<T extends Persistable>
{
	public static final String MERGE_STRATEGY_KEY = "MERGE_STRATEGY";
	private RootMergeStrategy<T> mergeStrategy;

	public Mergeable transform(
			final Key key,
			final Mergeable mergeable ) {
		if ((mergeable != null) && (mergeable instanceof RasterTile)) {
			final RasterTile<T> rasterTile = (RasterTile) mergeable;
			return new MergeableRasterTile<T>(
					rasterTile.getDataBuffer(),
					rasterTile.getMetadata(),
					mergeStrategy,
					new ByteArrayId(
							key.getColumnFamily().getBytes()));
		}
		return mergeable;
	}

	public void init(
			final Map<String, String> options )
			throws IOException {
		final String mergeStrategyStr = options.get(MERGE_STRATEGY_KEY);
		if (mergeStrategyStr != null) {
			final byte[] mergeStrategyBytes = ByteArrayUtils.byteArrayFromString(mergeStrategyStr);
			mergeStrategy = PersistenceUtils.fromBinary(
					mergeStrategyBytes,
					RootMergeStrategy.class);
		}
	}
}
