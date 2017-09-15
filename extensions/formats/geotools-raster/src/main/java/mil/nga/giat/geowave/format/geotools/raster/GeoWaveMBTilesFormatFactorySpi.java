package mil.nga.giat.geowave.format.geotools.raster;

import java.awt.RenderingHints.Key;
import java.util.Collections;
import java.util.Map;

import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridFormatFactorySpi;

public class GeoWaveMBTilesFormatFactorySpi implements
		GridFormatFactorySpi
{

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		return Collections.emptyMap();
	}

	@Override
	public AbstractGridFormat createFormat() {
		return new GeoWaveMBTilesFormat();
	}

}
