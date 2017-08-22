package mil.nga.giat.geowave.service.rest.webapp;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.logging.Level;

import javax.servlet.ServletContext;

import org.reflections.Reflections;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.engine.Engine;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.service.CorsService;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.service.rest.MainResource;
import mil.nga.giat.geowave.service.rest.GeoWaveOperationFinder;
import mil.nga.giat.geowave.service.rest.RestRoute;
import mil.nga.giat.geowave.service.rest.SwaggerApiParser;
import mil.nga.giat.geowave.service.rest.SwaggerResource;

/**
 * This class provides the main webapp entry point
 */
public class ApiRestletApplication extends
		Application
{
	private ArrayList<RestRoute> availableRoutes = null;
	private ArrayList<String> unavailableCommands = null;

	public ApiRestletApplication() {
		super();

		// Engine.setRestletLogLevel(Level.FINEST);
		parseOperationsForApiRoutes();

		// add the CORS service so others can access the service
		CorsService corsService = new CorsService();
		corsService.setAllowedOrigins(
				new HashSet(
						Arrays.asList(
								"*")));
		corsService.setAllowedCredentials(
				true);
		this.getServices().add(
				corsService);
	}

	@Override
	public synchronized Restlet createInboundRoot() {

		// Create a router Restlet and map all the resources
		Router router = new Router(
				getContext());

		// set context attributes that resources may need access to here
		getContext().getAttributes().put(
				"availableRoutes",
				availableRoutes);
		getContext().getAttributes().put(
				"unavailableCommands",
				unavailableCommands);

		final ServletContext servlet = (ServletContext) getContext().getAttributes().get(
				"org.restlet.ext.servlet.ServletContext");
		final String realPath = servlet.getRealPath(
				"/");
		getContext().getAttributes().put(
				"databaseUrl",
				"jdbc:sqlite:" + realPath + "api.db");

		// actual mapping here
		router.attachDefault(
				MainResource.class);
		router.attach(
				"/api",
				SwaggerResource.class);
		attachApiRoutes(
				router);

		initApiKeyDatabase(
				realPath + "api.db");
		return router;
	}

	public void initApiKeyDatabase(
			String fileName ) {

		String url = "jdbc:sqlite:" + fileName;

		try (Connection conn = DriverManager.getConnection(
				url)) {
			if (conn != null) {
				// SQL statement for creating a new table
				String sql = "CREATE TABLE IF NOT EXISTS api_keys (\n" + "	id integer PRIMARY KEY,\n"
						+ "	apiKey blob NOT NULL,\n" + "	username text NOT NULL\n" + ");";

				Statement stmnt = conn.createStatement();
				stmnt.execute(
						sql);
				stmnt.close();
			}
		}
		catch (SQLException e) {
			getContext().getLogger().log(
					Level.SEVERE,
					e.getMessage());
		}
	}

	/**
	 * This method parses all the Geowave Operation classes and creates the info
	 * to generate a Restlet route based on the operation. These routes are
	 * stored in the corresponding member variables including those that are
	 * unavailable
	 */
	public void parseOperationsForApiRoutes() {
		availableRoutes = new ArrayList<RestRoute>();
		unavailableCommands = new ArrayList<String>();

		for (final Class<?> operation : new Reflections(
				"mil.nga.giat.geowave").getTypesAnnotatedWith(
						GeowaveOperation.class)) {
			if ((operation.getAnnotation(
					GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.GET)
					|| (((operation.getAnnotation(
							GeowaveOperation.class).restEnabled() == GeowaveOperation.RestEnabledType.POST))
							&& DefaultOperation.class.isAssignableFrom(
									operation))
					|| ServerResource.class.isAssignableFrom(
							operation)) {

				availableRoutes.add(
						new RestRoute(
								operation));
			}
			else {
				final GeowaveOperation operationInfo = operation.getAnnotation(
						GeowaveOperation.class);
				unavailableCommands.add(
						operation.getName() + " " + operationInfo.name());
			}
		}

		Collections.sort(
				availableRoutes);
	}

	/**
	 * This method takes all the routes that were parsed and actually attaches
	 * them to the router. It also generates the swagger definition file.
	 */
	public void attachApiRoutes(
			Router router ) {
		final SwaggerApiParser apiParser = new SwaggerApiParser(
				"localhost:8080/geowave-service-rest-webapp",
				"1.0.0",
				"GeoWave API",
				"REST API for GeoWave CLI commands");
		for (final RestRoute route : availableRoutes) {

			if (DefaultOperation.class.isAssignableFrom(
					route.getOperation())) {
				router.attach(
						"/" + route.getPath(),
						new GeoWaveOperationFinder(
								(Class<? extends DefaultOperation<?>>) route.getOperation()));

				final Class<? extends DefaultOperation<?>> opClass = ((Class<? extends DefaultOperation<?>>) route
						.getOperation());

				apiParser.addRoute(
						route);

			}
			else {
				router.attach(
						route.getPath(),
						(Class<? extends ServerResource>) route.getOperation());
			}
		}

		// determine path on file system where the servlet resides
		// so we can serialize the swagger api json file to the correct location
		final ServletContext servlet = (ServletContext) router.getContext().getAttributes().get(
				"org.restlet.ext.servlet.ServletContext");
		final String realPath = servlet.getRealPath(
				"/");

		if (!apiParser.serializeSwaggerJson(
				realPath + "swagger.json"))
			getLogger().warning(
					"Serialization of swagger.json Failed");
		else
			getLogger().info(
					"Serialization of swagger.json Succeeded");
	}

}
