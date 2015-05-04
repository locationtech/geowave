package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.awt.image.SampleModel;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.index.Persistable;

import org.opengis.coverage.grid.GridCoverage;

public interface RasterTileMergeStrategy<T extends Persistable> extends
		Persistable
{
	public void merge(
			RasterTile<T> thisTile,
			RasterTile<T> nextTile,
			SampleModel sampleModel );

	public T getMetadata(
			GridCoverage tileGridCoverage,
			RasterDataAdapter dataAdapter );
}
