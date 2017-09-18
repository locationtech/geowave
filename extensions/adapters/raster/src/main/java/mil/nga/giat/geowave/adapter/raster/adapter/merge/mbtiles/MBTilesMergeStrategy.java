package mil.nga.giat.geowave.adapter.raster.adapter.merge.mbtiles;

import java.awt.image.WritableRaster;

import org.opengis.coverage.grid.GridCoverage;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.SimpleAbstractMergeStrategy;

public class MBTilesMergeStrategy extends
		SimpleAbstractMergeStrategy<MBTilesMetadata>
{

	@Override
	protected double getSample(
			int x,
			int y,
			int b,
			double thisSample,
			double nextSample ) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public MBTilesMetadata getMetadata(
			final GridCoverage tileGridCoverage,
			final RasterDataAdapter dataAdapter ) {
		// TODO Create and return MBTiles metadata here
		return null;
	}

	// TODO need to override this method, but running into errors when trying to
	// do so
	/*
	 * @Override protected void mergeRasters( final RasterTile<T> thisTile,
	 * final RasterTile<T> nextTile, final WritableRaster thisRaster, final
	 * WritableRaster nextRaster) {
	 * 
	 * }
	 */

}
