package mil.nga.giat.geowave.format.geotools.raster;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.factory.Hints;
import org.geotools.mbtiles.mosaic.MBTilesFormat;
import org.geotools.parameter.DefaultParameterDescriptorGroup;
import org.geotools.parameter.ParameterGroup;
import org.geotools.util.logging.Logging;
import org.opengis.parameter.GeneralParameterDescriptor;

public class GeoWaveMBTilesFormat extends
		MBTilesFormat
{
	private final static Logger LOGGER = Logging.getLogger(GeoWaveMBTilesFormat.class.getPackage().getName());

	@Override
	public AbstractGridCoverage2DReader getReader(
			Object source,
			Hints hints ) {
		try {
			return new GeoWaveMBTilesReader(
					source,
					hints);
		}
		catch (IOException e) {
			LOGGER.log(
					Level.WARNING,
					e.getLocalizedMessage(),
					e);
			return null;
		}
	}

	public GeoWaveMBTilesFormat() {
		setInfo();
	}

	private void setInfo() {
		final HashMap<String, String> info = new HashMap<String, String>();
		info.put(
				"name",
				"GWMBTiles");
		info.put(
				"description",
				"GeoWave MBTiles plugin");
		info.put(
				"vendor",
				"Locationtech");
		info.put(
				"docURL",
				"");
		info.put(
				"version",
				"1.0");
		mInfo = info;

		// reading parameters
		readParameters = new ParameterGroup(
				new DefaultParameterDescriptorGroup(
						mInfo,
						new GeneralParameterDescriptor[] {
							READ_GRIDGEOMETRY2D
						}));
		writeParameters = null;
	}

}
