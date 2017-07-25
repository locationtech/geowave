package mil.nga.giat.geowave.service.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Level;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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
import org.restlet.ext.apispark.internal.conversion.swagger.v2_0.Swagger2Reader;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.ext.swagger.SwaggerApplication;
import org.restlet.ext.swagger.SwaggerSpecificationRestlet;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.Representation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

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
		final StringBuilder routeStringBuilder = new StringBuilder("Available Routes:<br>");

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

		Writer writer = null;
		try {
			writer = new FileWriter("Output.json");    
		} catch (IOException e) {
			e.printStackTrace();
		}
		Gson gson = new GsonBuilder().create();
		JsonObject routes_json = new JsonObject();
		for (final Route route : availableRoutes) {
			
			//if (parser != null)
			//	parser.parseParameters();

			if (DefaultOperation.class.isAssignableFrom(route.getOperation())) {
				router.attach(
						route.getPath(),
						new GeoWaveOperationFinder(
								(Class<? extends DefaultOperation<?>>) route.getOperation()));

				
				Class<? extends DefaultOperation<?>> opClass = ((Class<? extends DefaultOperation<?>>) route.getOperation());
				SwaggerRestParser parser = null;
				try {
					System.out.println("OPERATION: "+route.getPath()+" : "+opClass.getName());
					
					parser = new SwaggerRestParser<>(opClass.newInstance());
					JsonObject op_json = ((SwaggerRestParser)parser).GetJsonObject();
					
					JsonObject method_json = new JsonObject();
					String method = route.getOperation().getAnnotation(GeowaveOperation.class).restEnabled().toString();
					method_json.add(method.toLowerCase(), op_json);
					
					routes_json.add("/"+route.getPath(), method_json);
				}
				catch (InstantiationException | IllegalAccessException e) {
					getLogger().log(
							Level.WARNING,
							"Exception while instantiating the geowave operation server resource.",
							e);
				}
			}
			else {
				router.attach(
						route.getPath(),
						(Class<? extends ServerResource>) route.getOperation());
			}
		}
		
		String header =  "{\"swagger\": \"2.0\","+
				  "\"info\": {"+
				    "\"version\": \"1.0.0\","+
				    "\"title\": \"GeoWave API\","+
				    "\"description\": \"REST API for GeoWave CLI commands\","+
				    "\"termsOfService\": \"http://localhost:5152/\","+
				    "\"contact\": {"+
				      "\"name\": \"GeoWave Team\""+
				    "},"+
				    "\"license\": {"+
				      "\"name\": \"MIT\""+
				    "}"+
				  "},"+
				  "\"host\": \"localhost:5152\","+
				  "\"basePath\": \"/\","+
				  "\"schemes\": ["+
				    "\"http\""+
				  "],"+
				  "\"consumes\": ["+
				    "\"application/json\""+
				  "],"+
				  "\"produces\": ["+
				    "\"application/json\""+
				  "],"+
				  "\"paths\":";
		
	    try {
			writer.write(header);
			gson.toJson(routes_json, writer);
			writer.write("}");
			writer.close();  
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
		// Provide basic 404 error page for unknown route
		router.attach("geo/mycontacts", ContactsServerResource.class);
		router.attachDefault(RestServer.class);

		// Setup router
		final Application myApp = new SwaggerApplication() {


			@Override
			public Restlet createInboundRoot() {
				router.setContext(getContext());

				
				attachSwaggerSpecificationRestlet(
						router,
						"docs");
				return router;
			};
			
			@Override
			public String getName() {
				return "GeoWave API";
			}
			
			@Override
			public SwaggerSpecificationRestlet getSwaggerSpecificationRestlet(
			        Context context) {
			    return new SwaggerSpecificationRestlet(getContext()) {
			        @Override
			        public Representation getApiDeclaration(String category) {
			            JacksonRepresentation result = new JacksonRepresentation(
			                    new FileRepresentation("./output.json/" + category,
			                            MediaType.APPLICATION_JSON),
			                        ApiDeclaration.class);
			            return result;
			        }
			        @Override
			        public Representation getResourceListing() {
			            JacksonRepresentation result = new JacksonRepresentation(
			                    new FileRepresentation("./output.json",
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
		public Route(final Class<?> operation ) {
			this.path = pathFor(operation).substring(1);
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
		public static String pathFor(final Class<?> operation ) {

			// Top level of hierarchy
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
