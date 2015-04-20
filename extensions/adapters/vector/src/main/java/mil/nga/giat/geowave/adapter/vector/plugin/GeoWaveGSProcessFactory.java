package mil.nga.giat.geowave.adapter.vector.plugin;

import org.geotools.process.factory.AnnotatedBeanProcessFactory;
import org.geotools.text.Text;

/**
 * This is the GeoTools Factory for introducing the nga:Decimation rendering
 * transform. GeoTools uses Java SPI to inject the WPS process (see
 * META-INF/services/org.geotools.process.ProcessFactory).
 * 
 */
public class GeoWaveGSProcessFactory extends
		AnnotatedBeanProcessFactory
{

	public GeoWaveGSProcessFactory() {
		super(
				Text.text("GeoWave Process Factory"),
				"nga",
				DecimationProcess.class);
	}

}
