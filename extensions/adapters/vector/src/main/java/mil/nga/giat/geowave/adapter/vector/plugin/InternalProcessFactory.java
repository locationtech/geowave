package mil.nga.giat.geowave.adapter.vector.plugin;

import org.geotools.process.factory.AnnotatedBeanProcessFactory;
import org.geotools.text.Text;

import mil.nga.giat.geowave.adapter.vector.render.InternalDistributedRenderProcess;

public class InternalProcessFactory extends
		AnnotatedBeanProcessFactory
{

	public InternalProcessFactory() {
		super(
				Text.text("Internal GeoWave Process Factory"),
				"internal",
				InternalDistributedRenderProcess.class);
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

}
