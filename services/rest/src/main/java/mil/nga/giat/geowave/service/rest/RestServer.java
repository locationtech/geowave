package mil.nga.giat.geowave.service.rest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.reflections.Reflections;
import org.shaded.restlet.security.Verifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.shaded.restlet.security.SecretVerifier;
import org.shaded.restlet.security.Authenticator;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import org.shaded.restlet.Application;
import org.shaded.restlet.Component;
import org.shaded.restlet.Server;
import org.shaded.restlet.data.Protocol;
import org.shaded.restlet.resource.Get;
import org.shaded.restlet.resource.ServerResource;
import org.shaded.restlet.routing.Router;
import org.shaded.restlet.Restlet;

import org.shaded.restlet.Restlet;
import org.shaded.restlet.security.ChallengeAuthenticator;
import org.shaded.restlet.data.ChallengeScheme;
import org.shaded.restlet.security.MapVerifier;
import org.shaded.restlet.Context;

public class RestServer extends
		ServerResource
{
	private ArrayList<Route> availableRoutes;

	private final static String DEFAULT_USERNAME = "admin", DEFAULT_PASSWORD = "password";

	private final static Logger LOGGER = LoggerFactory.getLogger(RestServer.class);

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

			private void initializePasswordsFile(
					File passwordsFile ) {

				try (PrintWriter writer = new PrintWriter(
						new OutputStreamWriter(
								new FileOutputStream(
										passwordsFile),
								StandardCharsets.UTF_8))) {
					LOGGER.info(
							"Initializing passwords file at {}. Default is {}/{}",
							new Object[] {
								passwordsFile,
								DEFAULT_USERNAME,
								DEFAULT_PASSWORD
							});
					writer.println(DEFAULT_USERNAME + "," + DigestUtils.sha256Hex(DEFAULT_PASSWORD));
				}
				catch (IOException e) {
					throw new RuntimeException(
							e);
				}
			}

			private Authenticator createAuthenticator() {
				ChallengeAuthenticator authenticator = new ChallengeAuthenticator(
						getContext(),
						ChallengeScheme.HTTP_BASIC,
						"geowave");

				File passwordsFile = new File(
						ConfigOptions.getDefaultPropertyPath(),
						"passwords.csv");
				if (!passwordsFile.exists()) initializePasswordsFile(passwordsFile);

				Map<String, String> passwords = new HashMap<>();
				try (CSVParser parser = CSVParser.parse(
						passwordsFile,
						StandardCharsets.UTF_8,
						CSVFormat.DEFAULT)) {
					for (CSVRecord record : parser) {
						String username = record.get(0);
						String passwordDigest = record.get(1);
						passwords.put(
								username,
								passwordDigest);
					}
				}
				catch (IOException e) {
					throw new RuntimeException(
							e);
				}

				SecretVerifier verifier = new SecretVerifier() {
					@Override
					public int verify(
							String identifier,
							char[] secret ) {
						String s = passwords.get(identifier);
						return (s != null && s.equals(DigestUtils.sha256Hex(new String(
								secret)))) ? RESULT_VALID : RESULT_INVALID;
					}
				};

				authenticator.setVerifier(verifier);

				return authenticator;
			}

			@Override
			public Restlet createInboundRoot() {
				router.setContext(getContext());

				Authenticator authenticator = createAuthenticator();
				authenticator.setNext(router);

				return authenticator;
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
