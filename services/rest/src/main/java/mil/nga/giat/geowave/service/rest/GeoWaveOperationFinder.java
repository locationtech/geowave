package mil.nga.giat.geowave.service.rest;

import java.util.logging.Level;

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
		try {
			return new GeoWaveOperationServiceWrapper<>(
					operation.getClass().newInstance(),
					defaultConfigFile);
		} catch (InstantiationException | IllegalAccessException e) {
			getLogger().log(
					Level.SEVERE,
					"Unable to instantiate Service Resource",
					e);
			return null;
			
		}
	}

	@Override
	public Class<? extends ServerResource> getTargetClass() {
		return GeoWaveOperationServiceWrapper.class;
	}

}
