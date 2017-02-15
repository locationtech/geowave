package mil.nga.giat.geowave.adapter.raster.plugin.gdal;

import org.geotools.coverageio.gdal.BaseGDALGridCoverage2DReader;
import org.geotools.data.DataSourceException;
import org.geotools.factory.Hints;
import org.opengis.coverage.grid.Format;
import org.opengis.coverage.grid.GridCoverageReader;

import it.geosolutions.imageio.plugins.geotiff.GeoTiffImageReaderSpi;

public class GDALGeoTiffReader extends
		BaseGDALGridCoverage2DReader implements
		GridCoverageReader
{
	private final static String worldFileExt = "";

	/**
	 * Creates a new instance of a {@link DTEDReader}. I assume nothing about
	 * file extension.
	 * 
	 * @param input
	 *            Source object for which we want to build an {@link DTEDReader}
	 *            .
	 * @throws DataSourceException
	 */
	public GDALGeoTiffReader(
			Object input )
			throws DataSourceException {
		this(
				input,
				null);
	}

	/**
	 * Creates a new instance of a {@link DTEDReader}. I assume nothing about
	 * file extension.
	 * 
	 * @param input
	 *            Source object for which we want to build an {@link DTEDReader}
	 *            .
	 * @param hints
	 *            Hints to be used by this reader throughout his life.
	 * @throws DataSourceException
	 */
	public GDALGeoTiffReader(
			Object input,
			Hints hints )
			throws DataSourceException {
		super(
				input,
				hints,
				worldFileExt,
				new GeoTiffImageReaderSpi());
	}

	/**
	 * @see org.opengis.coverage.grid.GridCoverageReader#getFormat()
	 */
	public Format getFormat() {
		return new GDALGeoTiffFormat();
	}

}
