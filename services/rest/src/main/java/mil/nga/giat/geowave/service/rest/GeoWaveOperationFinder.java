package mil.nga.giat.geowave.service.rest;

import org.restlet.Request;
import org.restlet.Response;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

public class GeoWaveOperationFinder extends
		Finder
{
	private final ServiceEnabledCommand<?> operation;
	private final String defaultConfigFile;

	public GeoWaveOperationFinder(
			final ServiceEnabledCommand<?> operation,
			String defaultConfigFile ) {
		this.operation = operation;
		this.defaultConfigFile = defaultConfigFile;

	}

	@Override
	public ServerResource create(
			final Class<? extends ServerResource> targetClass,
			final Request request,
			final Response response ) {
		return new GeoWaveOperationServiceWrapper<>(
				operation,
				defaultConfigFile);
	}

	@Override
	public Class<? extends ServerResource> getTargetClass() {
		return GeoWaveOperationServiceWrapper.class;
	}

}
