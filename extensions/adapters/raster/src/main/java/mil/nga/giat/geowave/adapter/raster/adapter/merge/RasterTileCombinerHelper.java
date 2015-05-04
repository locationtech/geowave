package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

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
