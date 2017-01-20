package mil.nga.giat.geowave.adapter.raster.plugin;

import java.awt.RenderingHints.Key;
import java.util.Map;

import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridFormatFactorySpi;

public class GeoWaveGTRasterFormatFactory implements
		GridFormatFactorySpi
{

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		return null;
	}

	@Override
	public AbstractGridFormat createFormat() {
		return new GeoWaveGTRasterFormat();
	}

}
