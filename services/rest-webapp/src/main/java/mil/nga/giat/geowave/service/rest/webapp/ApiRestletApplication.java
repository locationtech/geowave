package mil.nga.giat.geowave.service.rest.webapp;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.servlet.ServletContext;

import org.reflections.Reflections;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.restlet.service.CorsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.service.rest.GeoWaveOperationFinder;
import mil.nga.giat.geowave.service.rest.MainResource;
import mil.nga.giat.geowave.service.rest.RestRoute;
import mil.nga.giat.geowave.service.rest.SwaggerApiParser;
import mil.nga.giat.geowave.service.rest.SwaggerResource;
import mil.nga.giat.geowave.service.rest.operations.FileUpload;
import scala.reflect.api.Quasiquotes.Quasiquote.api;

/**
 * This class provides the main webapp entry point
 */
public class ApiRestletApplication extends
		Application
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ApiRestletApplication.class);
	private ArrayList<RestRoute> availableRoutes = null;

	public ApiRestletApplication() {
		super();

		// Engine.setRestletLogLevel(Level.FINEST);
		parseOperationsForApiRoutes();

		// add the CORS service so others can access the service
		final CorsService corsService = new CorsService();
		corsService.setAllowedOrigins(new HashSet(
				Arrays.asList("*")));
		corsService.setAllowedCredentials(true);
		getServices().add(
				corsService);
	}

	@Override
	public synchronized Restlet createInboundRoot() {

		// Create a router Restlet and map all the resources
		final Router router = new Router(
				getContext());

		// set context attributes that resources may need access to here
		getContext().getAttributes().put(
				"availableRoutes",
				availableRoutes);

		// actual mapping here
		router.attachDefault(MainResource.class);
		router.attach(
				"/api",
				SwaggerResource.class);
		router.attach(
				"/v0/fileupload",
				FileUpload.class);
		attachApiRoutes(router);
		return router;
	}

	/**
	 * This method parses all the Geowave Operation classes and creates the info
	 * to generate a Restlet route based on the operation. These routes are
	 * stored in the corresponding member variables including those that are
	 * unavailable
	 */
	public void parseOperationsForApiRoutes() {
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
				getLogger().log(
						Level.SEVERE,
						"Unable to instantiate Service Resource",
						e);
			}
		}

		Collections.sort(availableRoutes);
	}

	/**
	 * This method takes all the routes that were parsed and actually attaches
	 * them to the router. It also generates the swagger definition file.
	 */
	public void attachApiRoutes(
			final Router router ) {
		final ServletContext servlet = (ServletContext) router.getContext().getAttributes().get(
				"org.restlet.ext.servlet.ServletContext");
		// TODO document that this can be provided rather than discovered used
		// this servlet init param
		String apiHostPort = servlet.getInitParameter("host_port");
		if (apiHostPort == null) {
			try {
				apiHostPort = getHTTPEndPoint();
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to find httpo endpoint for swagger",
						e);
			}

		}
		final SwaggerApiParser apiParser = new SwaggerApiParser(
				apiHostPort,
				servlet.getContextPath(),
				VersionUtils.getVersion(),
				"GeoWave API",
				"REST API for GeoWave CLI commands");
		for (final RestRoute route : availableRoutes) {
			router.attach(
					"/" + route.getPath(),
					new GeoWaveOperationFinder(
							route.getOperation()));

			apiParser.addRoute(route);

		}

		// determine path on file system where the servlet resides
		// so we can serialize the swagger api json file to the correct location
		final String realPath = servlet.getRealPath("/");

		if (!apiParser.serializeSwaggerJson(realPath + "swagger.json")) {
			getLogger().warning(
					"Serialization of swagger.json Failed");
		}
		else {
			getLogger().info(
					"Serialization of swagger.json Succeeded");
		}
	}

	private static String getHTTPEndPoint()
			throws Exception {
		final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		final QueryExp subQuery1 = Query.match(
				Query.attr("protocol"),
				Query.value("HTTP/1.1"));
		final QueryExp subQuery2 = Query.anySubString(
				Query.attr("protocol"),
				Query.value("Http11"));
		final QueryExp query = Query.or(
				subQuery1,
				subQuery2);
		final Set<ObjectName> objs = mbs.queryNames(
				new ObjectName(
						"*:type=Connector,*"),
				query);
		final String hostname = InetAddress.getLocalHost().getHostName();
		final InetAddress[] addresses = InetAddress.getAllByName(hostname);
		for (final Iterator<ObjectName> i = objs.iterator(); i.hasNext();) {
			final ObjectName obj = i.next();
			// final String scheme = mbs.getAttribute(
			// obj,
			// "scheme").toString();
			final String port = obj.getKeyProperty("port");
			for (final InetAddress addr : addresses) {
				if (addr.isAnyLocalAddress() || addr.isLoopbackAddress() || addr.isMulticastAddress()) {
					continue;
				}
				final String host = addr.getHostAddress();
				// just return the first one
				return host + ":" + port;
			}
			return hostname + ":" + port;
		}
		return "localhost:8080";
	}
}
