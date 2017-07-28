package mil.nga.giat.geowave.cli.geoserver;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

abstract public class GeoServerCommand<T> extends
		ServiceEnabledCommand<T>
{

	protected GeoServerRestClient geoserverClient = null;

	@Override
	public boolean prepare(
			final OperationParams params ) {
		if (geoserverClient == null) {
			// Create the rest client
			geoserverClient = GeoServerRestClient.getInstance(new GeoServerConfig(
					getGeoWaveConfigFile(params)));
		}

		// Successfully prepared
		return true;
	}
}
