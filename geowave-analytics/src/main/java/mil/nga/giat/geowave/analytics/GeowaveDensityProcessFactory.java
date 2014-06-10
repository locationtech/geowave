package mil.nga.giat.geowave.analytics;

import org.geotools.process.factory.AnnotatedBeanProcessFactory;
import org.geotools.text.Text;

public class GeowaveDensityProcessFactory extends
		AnnotatedBeanProcessFactory
{

	public GeowaveDensityProcessFactory() {
		super(
				Text.text("GeoWave Density Process Factory"),
				"nga",
				DensityProcess.class);
	}

}
