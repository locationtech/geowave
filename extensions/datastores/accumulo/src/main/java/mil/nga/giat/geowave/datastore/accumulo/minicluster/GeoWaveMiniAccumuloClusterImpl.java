package mil.nga.giat.geowave.datastore.accumulo.minicluster;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;

public class GeoWaveMiniAccumuloClusterImpl extends
		MiniAccumuloClusterImpl
{

	public GeoWaveMiniAccumuloClusterImpl(
			MiniAccumuloConfigImpl config )
			throws IOException {
		super(
				config);
	}

	public void setExternalShutdownExecutor(
			ExecutorService svc ) {
		// @formatter:off
		/*if[accumulo.api=1.6]
		// nothing here
		else[accumulo.api=1.6]*/
		setShutdownExecutor(svc);
		/*end[accumulo.api=1.6]*/
		// @formatter:on
	}
}
