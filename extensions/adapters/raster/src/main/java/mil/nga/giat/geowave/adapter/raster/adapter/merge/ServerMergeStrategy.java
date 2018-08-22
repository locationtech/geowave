package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.persist.Persistable;

public interface ServerMergeStrategy<T extends Persistable>
{
	public void merge(
			final RasterTile<T> thisTile,
			final RasterTile<T> nextTile,
			final short internalAdapterId );
}
