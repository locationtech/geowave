package mil.nga.giat.geowave.format.geotools.raster;

import java.io.IOException;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.factory.Hints;
import org.geotools.mbtiles.mosaic.MBTilesReader;
import org.opengis.parameter.GeneralParameterValue;

public class GeoWaveMBTilesReader extends
		MBTilesReader
{

	public GeoWaveMBTilesReader(
			final Object source,
			final Hints hints )
			throws IOException {
		super(
				source,
				hints);
	}

	@Override
	public GridCoverage2D read(
			final GeneralParameterValue[] parameters )
			throws IllegalArgumentException,
			IOException {
		// the current implementation of read does not use the constant coverage
		// name used by this reader, which is the primary reason for a GeoWave
		// extension to the MBTilesReader
		return new GridCoverage2D(
				coverageName,
				super.read(
						parameters));
	}
}
