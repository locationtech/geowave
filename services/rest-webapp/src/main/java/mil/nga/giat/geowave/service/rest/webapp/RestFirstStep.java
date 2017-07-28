package mil.nga.giat.geowave.service.rest.webapp;

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;

import mil.nga.giat.geowave.service.rest.RestServer;

public class RestFirstStep extends
		Application
{
	@Override
	public synchronized Restlet createInboundRoot() {
		// Create a router Restlet that routes each call to a
		// new instance of RestServer.
		Router router = new Router(
				getContext());
		RestServer restServer = new RestServer();
		// Defines only one route
		/*
		 * router.attach( "/geowave", RestResource.class);
		 */
		return restServer.run(router);
	}

}
