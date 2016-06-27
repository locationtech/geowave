package mil.nga.giat.geowave.format.landsat8;

import org.geotools.coverage.grid.GridCoverage2D;
import org.opengis.feature.simple.SimpleFeature;

public interface Landsat8BandConverterSpi
{
	public String getName();

	public GridCoverage2D convert(
			final String coverageName,
			final GridCoverage2D originalBandData,
			final SimpleFeature bandMetadata );
}
