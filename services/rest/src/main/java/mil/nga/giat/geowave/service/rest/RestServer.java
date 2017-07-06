package mil.nga.giat.geowave.service.rest;

import java.util.ArrayList;
import java.util.Collections;

import org.reflections.Reflections;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Restlet;
import org.restlet.Server;
import org.restlet.data.Protocol;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;

public class RestServer extends
		ServerResource
{
	private final ArrayList<Route> availableRoutes;
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
		availableRoutes = new ArrayList<Route>();
		unavailableCommands = new ArrayList<String>();

		for (final Class<?> operation : new Reflections(
				"mil.nga.giat.geowave").getTypesAnnotatedWith(GeowaveOperation.class)) {
			if ((operation.getAnnotation(
					GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.GET)
					|| (((operation.getAnnotation(
							GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.POST)) && DefaultOperation.class
							.isAssignableFrom(operation)) || ServerResource.class.isAssignableFrom(operation)) {

				availableRoutes.add(new Route(
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
		for (final Route route : availableRoutes) {
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
		for (final Route route : availableRoutes) {
			if (DefaultOperation.class.isAssignableFrom(route.getOperation())) {
				router.attach(
						route.getPath(),
						new GeoWaveOperationFinder(
								(Class<? extends DefaultOperation<?>>) route.getOperation()));
			}
			else {
				router.attach(
						route.getPath(),
						(Class<? extends ServerResource>) route.getOperation());
			}
		}
		// Provide basic 404 error page for unknown route
		router.attachDefault(RestServer.class);

		// Setup router
		final Application myApp = new Application() {

			@Override
			public Restlet createInboundRoot() {
				router.setContext(getContext());

				return router;
			};
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
	 * Holds necessary information to create a Restlet route
	 */
	private static class Route implements
			Comparable<Route>
	{
		private final String path;
		private final Class<?> operation;

		/**
		 * Create a new route given an operation
		 *
		 * @param operation
		 */
		public Route(
				final Class<?> operation ) {
			path = pathFor(
					operation).substring(
					1);
			this.operation = operation;
		}

		/**
		 * Return the operation as it was originally passed
		 *
		 * @return
		 */
		public Class<?> getOperation() {
			return operation;
		}

		/**
		 * Get the path that represents the route
		 *
		 * @return a string representing the path, specified by pathFor
		 */
		public String getPath() {
			return path;
		}

		/**
		 * Get the path for a command based on the operation hierarchy Return
		 * the path as a string in the format "/first/next/next"
		 *
		 * @param operation
		 *            - the operation to find the path for
		 * @return the formatted path as a string
		 */
		public static String pathFor(
				final Class<?> operation ) {
			// Top level of heirarchy
			if (operation == Object.class) {
				return "";
			}
			final GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
			return pathFor(operationInfo.parentOperation()) + "/" + operationInfo.name();
		}

		@Override
		public int compareTo(
				final Route route ) {
			return path.compareTo(route.path);
		}

		@Override
		public boolean equals(
				final Object route ) {
			return (route instanceof Route) && path.equals(((Route) route).path);
		}

		@Override
		public int hashCode() {
			return path.hashCode();
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
