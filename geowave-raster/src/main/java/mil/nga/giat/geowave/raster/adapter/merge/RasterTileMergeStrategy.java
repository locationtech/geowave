package mil.nga.giat.geowave.raster.adapter.merge;

import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.raster.adapter.RasterTile;

import org.opengis.coverage.grid.GridCoverage;

public interface RasterTileMergeStrategy<T extends Persistable> extends
		Mergeable
{
	public void merge(
			RasterTile<T> thisTile,
			RasterTile<T> nextTile );

	public T getMetadata(
			GridCoverage tileGridCoverage,
			RasterDataAdapter dataAdapter );
}
