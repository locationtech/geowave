package mil.nga.giat.geowave.service.rest;

import java.util.logging.Level;

import org.restlet.Request;
import org.restlet.Response;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;

import mil.nga.giat.geowave.core.cli.api.DefaultOperation;

public class GeoWaveOperationFinder extends
		Finder
{
	private final Class<? extends DefaultOperation<?>> operationClass;

	public GeoWaveOperationFinder(
			final Class<? extends DefaultOperation<?>> operationClass ) {
		this.operationClass = operationClass;
	}

	@Override
	public ServerResource create(
			final Class<? extends ServerResource> targetClass,
			final Request request,
			final Response response ) {
		ServerResource result = null;
		try {
			result = new GeoWaveOperationServiceWrapper<>(
					operationClass.newInstance());
		}
		catch (InstantiationException | IllegalAccessException e) {
			getLogger().log(
					Level.WARNING,
					"Exception while instantiating the geowave operation server resource.",
					e);
		}
		return result;
	}

	@Override
	public Class<? extends ServerResource> getTargetClass() {
		return GeoWaveOperationServiceWrapper.class;
	}

}
