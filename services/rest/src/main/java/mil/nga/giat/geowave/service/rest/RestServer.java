package mil.nga.giat.geowave.service.rest;

import java.util.ArrayList;

import org.reflections.Reflections;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;

import org.shaded.restlet.Application;
import org.shaded.restlet.Component;
import org.shaded.restlet.Server;
import org.shaded.restlet.data.Protocol;
import org.shaded.restlet.resource.Get;
import org.shaded.restlet.resource.ServerResource;
import org.shaded.restlet.routing.Router;
import org.shaded.restlet.Restlet;

public class RestServer extends
		ServerResource
{
	private ArrayList<Route> availableRoutes;

	/**
	 * Run the Restlet server (localhost:5152)
	 */
	public static void main(
			String[] args ) {
		RestServer server = new RestServer();
		server.run(5152);
	}

	public RestServer() {
		this.availableRoutes = new ArrayList<Route>();

		for (Class<?> operation : new Reflections(
				"mil.nga.giat.geowave").getTypesAnnotatedWith(GeowaveOperation.class)) {
			availableRoutes.add(new Route(
					operation));
		}
	}

	// Show a simple 404 if the route is unknown to the server
	@Get("html")
	public String toString() {
		StringBuilder routeStringBuilder = new StringBuilder(
				"Available Routes: (geowave/help is only that currently extends ServerResource)<br>");
		for (Route route : availableRoutes) {
			routeStringBuilder.append(route.getPath() + " -> " + route.getOperationAsGeneric() + "<br>");
		}
		return "<b>404</b>: Route not found<br><br>" + routeStringBuilder.toString();
	}

	public void run(
			int port ) {

		// Add paths for each command
		final Router router = new Router();
		for (Route route : availableRoutes) {
			if (route.isServerResource()) {
				router.attach(
						route.getPath(),
						route.getOperationAsResource());
			}
			else {
				router.attach(
						route.getPath(),
						NonResourceCommand.class);
			}
		}
		// Provide basic 404 error page for unknown route
		router.attachDefault(RestServer.class);

		// Setup router
		Application myApp = new Application() {
			@Override
			public Restlet createInboundRoot() {
				router.setContext(getContext());
				return router;
			};
		};
		Component component = new Component();
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
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Could not create Restlet server - is the port already bound?");
		}
	}

	/**
	 * Holds necessary information to create a Restlet route
	 */
	private static class Route
	{
		private String path;
		private Class<?> operation;
		private boolean serverResource;

		/**
		 * Create a new route given an operation
		 * 
		 * @param operation
		 */
		public Route(
				Class<?> operation ) {
			this.path = pathFor(
					operation).substring(
					1);
			this.operation = operation;

			// check if operation extends ServerResource, which is required for
			// Restlet to
			// generate a route from the class
			try {
				operation.asSubclass(ServerResource.class);
				serverResource = true;
			}
			catch (ClassCastException e) {
				serverResource = false;
			}
		}

		/**
		 * @return true if the route represents an operation which extends
		 *         ServerResource
		 */
		public boolean isServerResource() {
			return serverResource;
		}

		/**
		 * Return the operation as its ServerResource subclass if operation
		 * extends ServerResource, otherwise return null.
		 * 
		 * @return the operation as its ServerResource subclass, or null if
		 *         operation does not extend ServerResource
		 */
		public Class<? extends ServerResource> getOperationAsResource() {
			if (serverResource) {
				return operation.asSubclass(ServerResource.class);
			}
			else {
				return null;
			}
		}

		/**
		 * Return the operation as it was originally passed
		 * 
		 * @return
		 */
		public Class<?> getOperationAsGeneric() {
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
				Class<?> operation ) {
			// Top level of heirarchy
			if (operation == Object.class) {
				return "";
			}
			GeowaveOperation operationInfo = operation.getAnnotation(GeowaveOperation.class);
			return pathFor(operationInfo.parentOperation()) + "/" + operationInfo.name();
		}
	}

	/**
	 * A simple ServerResource to show if the route's operation does not extend
	 * ServerResource
	 */
	public static class NonResourceCommand extends
			ServerResource
	{
		@Get("html")
		public String toString() {
			return "The route exists, but the command does not extend ServerResource";
		}
	}
}
