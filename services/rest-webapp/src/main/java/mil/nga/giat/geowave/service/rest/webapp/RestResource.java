package mil.nga.giat.geowave.service.rest.webapp;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

public class RestResource extends
		ServerResource
{
	@Get
	public String represent() {
		return "GeoWave";
	}

}
