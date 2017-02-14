package mil.nga.giat.geowave.adapter.raster.plugin.gdal;

import java.awt.RenderingHints.Key;
import java.util.Map;

import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverageio.BaseGridFormatFactorySPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.geosolutions.imageio.plugins.geotiff.GeoTiffImageReaderSpi;

public class GDALGeoTiffFormatFactory extends
		BaseGridFormatFactorySPI
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GDALGeoTiffFormatFactory.class);

	@Override
	public boolean isAvailable() {
		boolean available = true;

		// if these classes are here, then the runtime environment has
		// access to JAI and the JAI ImageI/O toolbox.
		try {
			Class.forName("it.geosolutions.imageio.plugins.geotiff.GeoTiffImageReaderSpi");
			available = new GeoTiffImageReaderSpi().isAvailable();

		}
		catch (final ClassNotFoundException cnf) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("GDALGeoTiffFormatFactory is not availaible.");
			}

			available = false;
		}

		return available;
	}

	@Override
	public AbstractGridFormat createFormat() {
		return new GDALGeoTiffFormat();
	}

}
