package mil.nga.giat.geowave.raster.adapter.merge;

import java.awt.image.SampleModel;

import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.raster.adapter.RasterTile;

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
