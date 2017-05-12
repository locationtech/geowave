package mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata;

import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

import mil.nga.giat.geowave.adapter.raster.FitToIndexGridCoverage;
import mil.nga.giat.geowave.adapter.raster.adapter.MergeableRasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMetadata.SampleIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opengis.coverage.grid.GridCoverage;

public class NoDataMergeStrategy implements
		RasterTileMergeStrategy<NoDataMetadata>
{
	public NoDataMergeStrategy() {}

	private static final long serialVersionUID = 38473874l;
	private static final Logger LOGGER = LoggerFactory.getLogger(NoDataMergeStrategy.class);

	@Override
	public void merge(
			final RasterTile<NoDataMetadata> thisTile,
			final RasterTile<NoDataMetadata> nextTile,
			final SampleModel sampleModel ) {

		// this strategy aims for latest tile with data values, but where there
		// is no data in the latest and there is data in the earlier tile, it
		// fills the data from the earlier tile

		// if next tile is null or if this tile does not have metadata, just
		// keep this tile as is
		if ((nextTile != null) && (thisTile.getMetadata() != null)) {
			if (nextTile instanceof MergeableRasterTile) {
				final NoDataMetadata thisTileMetadata = thisTile.getMetadata();
				final NoDataMetadata nextTileMetadata = nextTile.getMetadata();

				final WritableRaster thisRaster = Raster.createWritableRaster(
						sampleModel,
						thisTile.getDataBuffer(),
						null);
				final WritableRaster nextRaster = Raster.createWritableRaster(
						sampleModel,
						nextTile.getDataBuffer(),
						null);
				final int maxX = thisRaster.getMinX() + thisRaster.getWidth();
				final int maxY = thisRaster.getMinY() + thisRaster.getHeight();
				boolean recalculateMetadata = false;
				for (int b = 0; b < thisRaster.getNumBands(); b++) {
					for (int x = thisRaster.getMinX(); x < maxX; x++) {
						for (int y = thisRaster.getMinY(); y < maxY; y++) {
							if (thisTileMetadata.isNoData(
									new SampleIndex(
											x,
											y,
											b),
									thisRaster.getSampleDouble(
											x,
											y,
											b))) {
								final double sample = nextRaster.getSampleDouble(
										x,
										y,
										b);
								if ((nextTileMetadata == null) || !nextTileMetadata.isNoData(
										new SampleIndex(
												x,
												y,
												b),
										sample)) {
									// we only need to recalculate metadata if
									// the raster is overwritten,
									// otherwise just use this raster's
									// metadata
									recalculateMetadata = true;
									thisRaster.setSample(
											x,
											y,
											b,
											sample);
								}
							}
						}
					}
				}
				if (recalculateMetadata) {
					thisTile.setMetadata(NoDataMetadataFactory.mergeMetadata(
							thisTileMetadata,
							thisRaster,
							nextTileMetadata,
							nextRaster));
				}
			}
		}
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return (int) serialVersionUID;
		// this looks correct based on behaviour of equals?!? should return the
		// same hash code for all instances
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

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
