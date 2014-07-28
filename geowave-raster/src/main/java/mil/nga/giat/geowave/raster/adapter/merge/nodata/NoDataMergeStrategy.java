package mil.nga.giat.geowave.raster.adapter.merge.nodata;

import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.raster.adapter.RasterTile;
import mil.nga.giat.geowave.raster.adapter.merge.AbstractMergeStrategy;
import mil.nga.giat.geowave.raster.adapter.merge.nodata.NoDataMetadata.SampleIndex;

import org.opengis.coverage.grid.GridCoverage;

public class NoDataMergeStrategy extends
		AbstractMergeStrategy<NoDataMetadata>
{

	protected NoDataMergeStrategy() {}

	public NoDataMergeStrategy(
			final ByteArrayId adapterId,
			final SampleModel sampleModel ) {
		super(
				adapterId,
				sampleModel);
	}

	@Override
	public void merge(
			final RasterTile<NoDataMetadata> thisTile,
			final RasterTile<NoDataMetadata> nextTile ) {
		// this strategy aims for latest tile with data values, but where there
		// is no data in the latest and there is data in the earlier tile, it
		// fills the data from the earlier tile
		if (nextTile != null) {
			if (nextTile.getMetadata() == null) {
				// just overwrite this tile's metadata and data with that of the
				// other tile
				thisTile.setDataBuffer(nextTile.getDataBuffer());
				thisTile.setMetadata(nextTile.getMetadata());
			}
			else if (nextTile instanceof MergeableRasterTile) {
				final NoDataMetadata otherTileMetadata = nextTile.getMetadata();
				final NoDataMetadata thisTileMetadata = thisTile.getMetadata();
				final SampleModel sampleModel = getSampleModel(((MergeableRasterTile) nextTile).getDataAdapterId());
				final WritableRaster otherRaster = Raster.createWritableRaster(
						sampleModel,
						nextTile.getDataBuffer(),
						null);
				final WritableRaster thisRaster = Raster.createWritableRaster(
						sampleModel,
						thisTile.getDataBuffer(),
						null);
				final int maxX = otherRaster.getMinX() + otherRaster.getWidth();
				final int maxY = otherRaster.getMinY() + otherRaster.getHeight();
				boolean recalculateMetadata = false;
				for (int b = 0; b < otherRaster.getNumBands(); b++) {
					for (int x = otherRaster.getMinX(); x < maxX; x++) {
						for (int y = otherRaster.getMinY(); y < maxY; y++) {
							if (otherTileMetadata.isNoData(
									new SampleIndex(
											x,
											y,
											b),
									otherRaster.getSampleDouble(
											x,
											y,
											b))) {
								final double sample = thisRaster.getSampleDouble(
										x,
										y,
										b);
								if ((thisTileMetadata == null) || !thisTileMetadata.isNoData(
										new SampleIndex(
												x,
												y,
												b),
										sample)) {
									// we only need to recalculate metadata if
									// the other raster is overwritten,
									// otherwise just use the other raster
									// metadata
									recalculateMetadata = true;
									otherRaster.setSample(
											x,
											y,
											b,
											sample);
								}
							}
						}
					}
				}
				thisTile.setDataBuffer(otherRaster.getDataBuffer());
				if (recalculateMetadata) {
					thisTile.setMetadata(NoDataMetadataFactory.mergeMetadata(
							thisTileMetadata,
							thisRaster,
							otherTileMetadata,
							otherRaster));
				}
				else {
					thisTile.setMetadata(otherTileMetadata);
				}
			}
		}
	}

	@Override
	public NoDataMetadata getMetadata(
			final GridCoverage tileGridCoverage,
			final RasterDataAdapter dataAdapter ) {
		if (tileGridCoverage instanceof FitToIndexGridCoverage) {
			return NoDataMetadataFactory.createMetadata(
					dataAdapter.getNoDataValuesPerBand(),
					((FitToIndexGridCoverage) tileGridCoverage).getFootprintScreenGeometry(),
					tileGridCoverage.getRenderedImage().getData());
		}
		return NoDataMetadataFactory.createMetadata(
				dataAdapter.getNoDataValuesPerBand(),
				null,
				tileGridCoverage.getRenderedImage().getData());
	}
}
