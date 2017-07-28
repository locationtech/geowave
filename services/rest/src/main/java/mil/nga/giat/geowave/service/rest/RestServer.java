package mil.nga.giat.geowave.service.rest;

import java.util.ArrayList;
import java.util.Collections;

import org.reflections.Reflections;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Server;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.ext.apispark.internal.conversion.swagger.v1_2.model.ApiDeclaration;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.ext.swagger.SwaggerApplication;
import org.restlet.ext.swagger.SwaggerSpecificationRestlet;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;

public class RestServer extends
		ServerResource
{
	private final ArrayList<RestRoute> availableRoutes;
	private final ArrayList<String> unavailableCommands;

	/**
	 * Run the Restlet server (localhost:5152)
	 */
	public static void main(
			final String[] args ) {
		final RestServer server = new RestServer();
		server.run(5152);
	}

	public RestServer() {
		availableRoutes = new ArrayList<RestRoute>();
		unavailableCommands = new ArrayList<String>();

		for (final Class<?> operation : new Reflections(
				"mil.nga.giat.geowave").getTypesAnnotatedWith(GeowaveOperation.class)) {
			if ((operation.getAnnotation(
					GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.GET)
					|| (((operation.getAnnotation(
							GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.POST)) && DefaultOperation.class
							.isAssignableFrom(operation)) || ServerResource.class.isAssignableFrom(operation)) {

				availableRoutes.add(new RestRoute(
						operation));
			}
			else {
				final GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
				unavailableCommands.add(operation.getName() + " " + operationInfo.name());
			}
		}

		Collections.sort(availableRoutes);
	}

	// Show a simple 404 if the route is unknown to the server
	@Get("html")
	public String listResources() {
		final StringBuilder routeStringBuilder = new StringBuilder(
				"Available Routes:<br>");

		for (final RestRoute route : availableRoutes) {
			routeStringBuilder.append(route.getPath() + " --> " + route.getOperation() + "<br>");
		}
		routeStringBuilder.append("<br><br><span style='color:blue'>Unavailable Routes:</span><br>");
		for (final String command : unavailableCommands) {
			routeStringBuilder.append("<span style='color:blue'>" + command + "</span><br>");
		}
		return "<b>404</b>: Route not found<br><br>" + routeStringBuilder.toString();
	}

	public void run(
			final int port ) {

		// Add paths for each command
		final Router router = new Router();

		SwaggerApiParser apiParser = new SwaggerApiParser("1.0.0", "GeoWave API", "REST API for GeoWave CLI commands");
		for (final RestRoute route : availableRoutes) {

			if (DefaultOperation.class.isAssignableFrom(route.getOperation())) {
				router.attach(
						route.getPath(),
						new GeoWaveOperationFinder(
								(Class<? extends DefaultOperation<?>>) route.getOperation()));

				Class<? extends DefaultOperation<?>> opClass = ((Class<? extends DefaultOperation<?>>) route
						.getOperation());
				
				apiParser.AddRoute(route);
			}
			else {
				router.attach(
						route.getPath(),
						(Class<? extends ServerResource>) route.getOperation());
			}
		}
		
		apiParser.SerializeSwaggerJson("swagger.json");

		// Provide basic 404 error page for unknown route
		router.attachDefault(RestServer.class);

		// Setup router
		final Application myApp = new SwaggerApplication() {

			@Override
			public Restlet createInboundRoot() {
				router.setContext(getContext());

				attachSwaggerSpecificationRestlet(
						router,
						"swagger.json");
				return router;
			};

			@Override
			public String getName() {
				return "GeoWave API";
			}

			@Override
			public SwaggerSpecificationRestlet getSwaggerSpecificationRestlet(
					Context context ) {
				return new SwaggerSpecificationRestlet(
						getContext()) {
					@Override
					public Representation getApiDeclaration(
							String category ) {
						JacksonRepresentation<ApiDeclaration> result = new JacksonRepresentation<ApiDeclaration>(
								new FileRepresentation(
										"./swagger.json/" + category,
										MediaType.APPLICATION_JSON),
								ApiDeclaration.class);
						return result;
					}

					@Override
					public Representation getResourceListing() {
						JacksonRepresentation<ApiDeclaration> result = new JacksonRepresentation<ApiDeclaration>(
								new FileRepresentation(
										"./swagger.json",
										MediaType.APPLICATION_JSON),
								ApiDeclaration.class);
						return result;
					}
				};
			}

		};
		final Component component = new Component();
		component.getDefaultHost().attach(
				"/",
				myApp);

		// Start server
		try {
			new Server(
					Protocol.HTTP,
					port,
					component).start();
		}
		catch (final Exception e) {
			e.printStackTrace();
			System.out.println("Could not create Restlet server - is the port already bound?");
		}
	}


	/**
	 * A simple ServerResource to show if the route's operation does not extend
	 * ServerResource
	 */
	public static class NonResourceCommand extends
			ServerResource
	{
		@Override
		@Get("html")
		public String toString() {
			return "The route exists, but the command does not extend ServerResource";
		}
	}
}
