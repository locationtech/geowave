package mil.nga.giat.geowave.vector.plugin;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import mil.nga.giat.geowave.vector.auth.AuthorizationFactorySPI;
import mil.nga.giat.geowave.vector.auth.EmptyAuthorizationFactory;
import mil.nga.giat.geowave.vector.plugin.lock.LockingManagementFactory;

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
	protected static final String ZOOKEEPER_SERVERS_KEY = "ZookeeperServers";
	protected static final String INSTANCE_NAME_KEY = "InstanceName";
	protected static final String USERNAME_KEY = "UserName";
	protected static final String PASSWORD_KEY = "Password";
	protected static final String ACCUMULO_NAMESPACE_KEY = "AccumuloNamespace";
        protected static final String FEATURE_NAMESPACE_KEY = "namespace";
	protected static final String LOCK_MGT_KEY = "Lock Management";
	protected static final String AUTH_MGT_KEY = "Authorization Management Provider";
	protected static final String AUTH_URL_KEY = "Authorization Data URL";

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
			"The table Accumulo namespace associated with this data store",
			true);
        
	public static final Param FEATURE_NAMESPACE = new Param(
                        FEATURE_NAMESPACE_KEY, 
                        URI.class,
                        "uri to a the namespace", 
                        false);

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
	private final String accumuloNamespace;
	private final String featureNamespace;        
	private final LockingManagementFactory lockingManagementFactory;
	private final AuthorizationFactorySPI authorizationFactory;
	private final URL authorizationURL;

	private static List<Param> accumuloParams = null;

	public static List<Param> getAuthPluginParams() {
		List<Param> accumuloParams = new ArrayList<Param>();
		accumuloParams.add(AUTH_MGT);
		accumuloParams.add(AUTH_URL);
		return accumuloParams;
	}

	public static List<Param> getPluginParams() {
		if (accumuloParams == null) {
			accumuloParams = new ArrayList<Param>();
			accumuloParams.add(ZOOKEEPER_SERVERS);
			accumuloParams.add(INSTANCE_NAME);
			accumuloParams.add(USERNAME);
			accumuloParams.add(PASSWORD);
			accumuloParams.add(ACCUMULO_NAMESPACE);
                        accumuloParams.add(FEATURE_NAMESPACE);
			accumuloParams.add(LOCK_MGT);
			accumuloParams.add(AUTH_MGT);
			accumuloParams.add(AUTH_URL);
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
					"Accumulo Plugin: Missing Accumulo namespace param");
		}
		accumuloNamespace = param.toString();
                
       		param = params.get(FEATURE_NAMESPACE_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"Accumulo Plugin: Missing feature namespace param");
		}
		featureNamespace = param.toString();

		param = params.get(LOCK_MGT_KEY);

		Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
		LockingManagementFactory factory = null;
		while (it.hasNext()) {
			factory = it.next();
			if (param == null || param.toString().equals(
					factory.toString())) break;
		}
		lockingManagementFactory = factory;

		authorizationFactory = getAuthorizationFactory(params);
		this.authorizationURL = getAuthorizationURL(params);
	}

	public static AuthorizationFactorySPI getAuthorizationFactory(
			final Map<String, Serializable> params )

			throws GeoWavePluginException {
		Serializable param = params.get(AUTH_MGT_KEY);
		Iterator<AuthorizationFactorySPI> authIt = getAuthorizationFactoryList();
		AuthorizationFactorySPI authFactory = new EmptyAuthorizationFactory();
		while (authIt.hasNext()) {
			authFactory = authIt.next();
			if (param == null || param.toString().equals(
					authFactory.toString())) break;
		}
		return authFactory;
	}

	public static URL getAuthorizationURL(
			final Map<String, Serializable> params )
			throws GeoWavePluginException {

		Serializable param = params.get(AUTH_URL_KEY);
		if (param == null) {
			return null;

		}
		else {
			try {
				return new URL(
						param.toString());
			}
			catch (MalformedURLException e) {

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
		return accumuloNamespace;
	}
        
	public String getFeatureNamespace() {
		return featureNamespace;
	}        

	private static Map<String, List<String>> getLockMgtOptions() {
		List<String> options = new ArrayList<String>();
		Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
		while (it.hasNext())
			options.add(it.next().toString());
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		map.put(
				Param.OPTIONS,
				options);
		return map;
	}

	private static Map<String, List<String>> getAuthSPIOptions() {
		List<String> options = new ArrayList<String>();
		Iterator<AuthorizationFactorySPI> it = getAuthorizationFactoryList();
		while (it.hasNext())
			options.add(it.next().toString());
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		map.put(
				Param.OPTIONS,
				options);
		return map;
	}

	private static Iterator<LockingManagementFactory> getLockManagementFactoryList() {
		ServiceLoader<LockingManagementFactory> ldr = ServiceLoader.load(LockingManagementFactory.class);
		return ldr.iterator();
	}

	private static Iterator<AuthorizationFactorySPI> getAuthorizationFactoryList() {
		ServiceLoader<AuthorizationFactorySPI> ldr = ServiceLoader.load(AuthorizationFactorySPI.class);
		return ldr.iterator();
	}
}
