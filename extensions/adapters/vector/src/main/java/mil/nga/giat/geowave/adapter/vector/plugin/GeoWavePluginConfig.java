package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.adapter.vector.auth.AuthorizationFactorySPI;
import mil.nga.giat.geowave.adapter.vector.auth.EmptyAuthorizationFactory;
import mil.nga.giat.geowave.adapter.vector.plugin.lock.LockingManagementFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.Parameter;

/**
 * This class encapsulates the parameterized configuration that can be provided
 * per GeoWave data store within GeoTools. For GeoServer this configuration can
 * be provided within the data store definition workflow.
 * 
 */
public class GeoWavePluginConfig
{
	private final static Logger LOGGER = Logger.getLogger(GeoWavePluginConfig.class);

	protected static final String ZOOKEEPER_SERVERS_KEY = "ZookeeperServers";
	protected static final String INSTANCE_NAME_KEY = "InstanceName";
	protected static final String USERNAME_KEY = "UserName";
	protected static final String PASSWORD_KEY = "Password";
	protected static final String ACCUMULO_NAMESPACE_KEY = "Namespace";
	// name matches the workspace parameter provided to the factory
	protected static final String FEATURE_NAMESPACE_KEY = "namespace";
	protected static final String LOCK_MGT_KEY = "Lock Management";
	protected static final String AUTH_MGT_KEY = "Authorization Management Provider";
	protected static final String AUTH_URL_KEY = "Authorization Data URL";
	protected static final String TRANSACTION_BUFFER_SIZE = "Transaction Buffer Size";

	private static final Param ZOOKEEPER_SERVERS = new Param(
			ZOOKEEPER_SERVERS_KEY,
			String.class,
			"A comma delimited list of zookeeper servers, as host:port",
			true);
	private static final Param INSTANCE_NAME = new Param(
			INSTANCE_NAME_KEY,
			String.class,
			"The Instance Name",
			true);
	private static final Param USERNAME = new Param(
			USERNAME_KEY,
			String.class,
			"The Username",
			true);
	private static final Param PASSWORD = new Param(
			PASSWORD_KEY,
			String.class,
			"The Password",
			true,
			"mypassword",
			Collections.singletonMap(
					Parameter.IS_PASSWORD,
					Boolean.TRUE));
	private static final Param ACCUMULO_NAMESPACE = new Param(
			ACCUMULO_NAMESPACE_KEY,
			String.class,
			"The table namespace associated with this data store",
			true);
	private static final Param TRANSACTION_BUFFER_SIZE_PARAM = new Param(
			TRANSACTION_BUFFER_SIZE,
			Integer.class,
			"Number of buffered buffered insertions before flush to the datastore.",
			false);

	/*
	 * private static final Param FEATURE_NAMESPACE = new Param(
	 * FEATURE_NAMESPACE_KEY, String.class,
	 * "The overriding namespace for all feature types maintained within this data store"
	 * , false);
	 */

	private static final Param LOCK_MGT = new Param(
			LOCK_MGT_KEY,
			String.class,
			"WFS-T Locking Support.",
			true,
			null,
			getLockMgtOptions());

	private static final Param AUTH_MGT = new Param(
			AUTH_MGT_KEY,
			String.class,
			"The provider to obtain authorization given a user.",
			true,
			null,
			getAuthSPIOptions());

	private static final Param AUTH_URL = new Param(
			AUTH_URL_KEY,
			String.class,
			"The providers data URL.",
			false);

	private final String zookeeperServers;
	private final String instanceName;
	private final String userName;
	private final String password;
	private final String namespace;
	private final URI featureNameSpaceURI;
	private final LockingManagementFactory lockingManagementFactory;
	private final AuthorizationFactorySPI authorizationFactory;
	private final URL authorizationURL;
	private final Integer transactionBufferSize;

	private static List<Param> accumuloParams = null;

	public static List<Param> getAuthPluginParams() {
		final List<Param> accumuloParams = new ArrayList<Param>();
		accumuloParams.add(AUTH_MGT);
		accumuloParams.add(AUTH_URL);
		return accumuloParams;
	}

	public synchronized static List<Param> getPluginParams() {
		if (accumuloParams == null) {
			accumuloParams = new ArrayList<Param>();
			accumuloParams.add(ZOOKEEPER_SERVERS);
			accumuloParams.add(INSTANCE_NAME);
			accumuloParams.add(USERNAME);
			accumuloParams.add(PASSWORD);
			accumuloParams.add(ACCUMULO_NAMESPACE);
			// is pulled from the workspace
			// accumuloParams.add(FEATURE_NAMESPACE);
			accumuloParams.add(LOCK_MGT);
			accumuloParams.add(AUTH_MGT);
			accumuloParams.add(AUTH_URL);
			accumuloParams.add(TRANSACTION_BUFFER_SIZE_PARAM);
		}
		return accumuloParams;
	}

	public GeoWavePluginConfig(
			final Map<String, Serializable> params )
			throws GeoWavePluginException {

		Serializable param = params.get(ZOOKEEPER_SERVERS_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"Accumulo Plugin: Missing zookeeper servers param");
		}

		zookeeperServers = param.toString();
		param = params.get(INSTANCE_NAME_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"Accumulo Plugin: Missing instance name param");
		}
		instanceName = param.toString();

		param = params.get(USERNAME_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"Accumulo Plugin: Missing username param");
		}

		userName = param.toString();

		param = params.get(PASSWORD_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"Accumulo Plugin: Missing password param");
		}
		password = param.toString();

		param = params.get(ACCUMULO_NAMESPACE_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"Accumulo Plugin: Missing namespace param");
		}
		namespace = param.toString();

		param = params.get(FEATURE_NAMESPACE_KEY);
		URI namespaceURI = null;
		if (param != null) {
			try {
				namespaceURI = param instanceof String ? new URI(
						param.toString()) : (URI) param;
			}
			catch (final URISyntaxException e) {
				LOGGER.error("Malformed Feature Namespace URI : " + param);
			}
		}
		featureNameSpaceURI = namespaceURI;

		param = params.get(TRANSACTION_BUFFER_SIZE);
		Integer bufferSizeFromParam = 10000;
		if (param != null) {
			try {
				bufferSizeFromParam = param instanceof Integer ? (Integer) param : Integer.parseInt(param.toString());
			}
			catch (final Exception e) {
				LOGGER.error("Malformed buffer size : " + param);
			}
		}
		transactionBufferSize = bufferSizeFromParam;

		param = params.get(LOCK_MGT_KEY);

		final Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
		LockingManagementFactory factory = null;
		while (it.hasNext()) {
			factory = it.next();
			if ((param == null) || param.toString().equals(
					factory.toString())) {
				break;
			}
		}
		lockingManagementFactory = factory;

		authorizationFactory = getAuthorizationFactory(params);
		authorizationURL = getAuthorizationURL(params);
	}

	public static AuthorizationFactorySPI getAuthorizationFactory(
			final Map<String, Serializable> params )

			throws GeoWavePluginException {
		final Serializable param = params.get(AUTH_MGT_KEY);
		final Iterator<AuthorizationFactorySPI> authIt = getAuthorizationFactoryList();
		AuthorizationFactorySPI authFactory = new EmptyAuthorizationFactory();
		while (authIt.hasNext()) {
			authFactory = authIt.next();
			if ((param == null) || param.toString().equals(
					authFactory.toString())) {
				break;
			}
		}
		return authFactory;
	}

	public static URL getAuthorizationURL(
			final Map<String, Serializable> params )
			throws GeoWavePluginException {

		final Serializable param = params.get(AUTH_URL_KEY);
		if (param == null) {
			return null;

		}
		else {
			try {
				return new URL(
						param.toString());
			}
			catch (final MalformedURLException e) {

				throw new GeoWavePluginException(
						"Accumulo Plugin: malformed Authorization Service URL " + param.toString());
			}
		}
	}

	protected AuthorizationFactorySPI getAuthorizationFactory() {
		return authorizationFactory;
	}

	protected URL getAuthorizationURL() {
		return authorizationURL;
	}

	public LockingManagementFactory getLockingManagementFactory() {
		return lockingManagementFactory;
	}

	public String getZookeeperServers() {
		return zookeeperServers;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}

	public String getAccumuloNamespace() {
		return namespace;
	}

	public URI getFeatureNamespace() {
		return featureNameSpaceURI;
	}

	public Integer getTransactionBufferSize() {
		return transactionBufferSize;
	}

	private static Map<String, List<String>> getLockMgtOptions() {
		final List<String> options = new ArrayList<String>();
		final Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
		while (it.hasNext()) {
			options.add(it.next().toString());
		}
		final Map<String, List<String>> map = new HashMap<String, List<String>>();
		map.put(
				Parameter.OPTIONS,
				options);
		return map;
	}

	private static Map<String, List<String>> getAuthSPIOptions() {
		final List<String> options = new ArrayList<String>();
		final Iterator<AuthorizationFactorySPI> it = getAuthorizationFactoryList();
		while (it.hasNext()) {
			options.add(it.next().toString());
		}
		final Map<String, List<String>> map = new HashMap<String, List<String>>();
		map.put(
				Parameter.OPTIONS,
				options);
		return map;
	}

	private static Iterator<LockingManagementFactory> getLockManagementFactoryList() {
		final ServiceLoader<LockingManagementFactory> ldr = ServiceLoader.load(LockingManagementFactory.class);
		return ldr.iterator();
	}

	private static Iterator<AuthorizationFactorySPI> getAuthorizationFactoryList() {
		final ServiceLoader<AuthorizationFactorySPI> ldr = ServiceLoader.load(AuthorizationFactorySPI.class);
		return ldr.iterator();
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option zookeeperUrl = new Option(
				"z",
				"zookeepers",
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		allOptions.addOption(zookeeperUrl);
		final Option instanceId = new Option(
				"i",
				"instance-id",
				true,
				"The Accumulo instance ID");
		allOptions.addOption(instanceId);
		final Option user = new Option(
				"u",
				"user",
				true,
				"A valid Accumulo user ID");
		allOptions.addOption(user);
		final Option password = new Option(
				"p",
				"password",
				true,
				"The password for the user");
		allOptions.addOption(password);

		final Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The table namespace (optional; default is no namespace)");
		allOptions.addOption(namespace);
	}

	public static GeoWavePluginConfig buildFromOptions(
			final CommandLine commandLine )
			throws ParseException,
			GeoWavePluginException {
		final Map<String, Serializable> params = new HashMap<String, Serializable>();
		params.put(
				ZOOKEEPER_SERVERS_KEY,
				commandLine.getOptionValue("z"));
		params.put(
				INSTANCE_NAME_KEY,
				commandLine.getOptionValue("i"));
		params.put(
				USERNAME_KEY,
				commandLine.getOptionValue("u"));
		params.put(
				PASSWORD_KEY,
				commandLine.getOptionValue("p"));
		params.put(
				ACCUMULO_NAMESPACE_KEY,
				commandLine.getOptionValue("n"));
		return new GeoWavePluginConfig(
				params);

	}

}
