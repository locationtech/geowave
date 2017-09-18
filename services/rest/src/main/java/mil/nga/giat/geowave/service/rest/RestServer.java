package mil.nga.giat.geowave.service.rest;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.reflections.Reflections;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Server;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.ext.apispark.internal.conversion.swagger.v1_2.model.ApiDeclaration;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.ext.swagger.SwaggerApplication;
import org.restlet.ext.swagger.SwaggerSpecificationRestlet;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.service.CorsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

public class RestServer extends
		ServerResource
{
	private static final Logger LOGGER = LoggerFactory.getLogger(RestServer.class);
	private final ArrayList<RestRoute> availableRoutes;

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

		for (final Class<? extends ServiceEnabledCommand> operation : new Reflections(
				"mil.nga.giat.geowave").getSubTypesOf(ServiceEnabledCommand.class)) {
			try {
				if (!Modifier.isAbstract(operation.getModifiers())) {
					availableRoutes.add(new RestRoute(
							operation.newInstance()));
				}
			}

			catch (InstantiationException | IllegalAccessException e) {
				LOGGER.error(
						"Unable to instantiate Service Resource",
						e);
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
		return "<b>404</b>: Route not found<br><br>" + routeStringBuilder.toString();
	}

	public void run(
			final int port ) {

		// Add paths for each command
		final Router router = new Router();

		final SwaggerApiParser apiParser = new SwaggerApiParser(
				"1.0.0",
				"GeoWave API",
				"REST API for GeoWave CLI commands");
		for (final RestRoute route : availableRoutes) {
			router.attach(
					route.getPath(),
					new GeoWaveOperationFinder(
							route.getOperation()));

			apiParser.addRoute(route);
		}
		router.attach(
				"fileupload",
				FileUpload.class);

		apiParser.serializeSwaggerJson("swagger.json");
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
					final Context context ) {
				return new SwaggerSpecificationRestlet(
						getContext()) {
					@Override
					public Representation getApiDeclaration(
							final String category ) {
						final JacksonRepresentation<ApiDeclaration> result = new JacksonRepresentation<ApiDeclaration>(
								new FileRepresentation(
										"./swagger.json/" + category,
										MediaType.APPLICATION_JSON),
								ApiDeclaration.class);
						return result;

					}

					@Override
					public Representation getResourceListing() {
						final JacksonRepresentation<ApiDeclaration> result = new JacksonRepresentation<ApiDeclaration>(
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
		// TODO I don't know exactly what we want to do, but I added this for my
		// ease at the moment
		final CorsService corsService = new CorsService();
		corsService.setAllowedOrigins(new HashSet(
				Arrays.asList("*")));
		corsService.setAllowedCredentials(true);
		myApp.getServices().add(
				corsService);
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

		// return router;
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
