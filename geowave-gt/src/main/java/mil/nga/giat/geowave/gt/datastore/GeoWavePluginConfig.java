package mil.nga.giat.geowave.gt.datastore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
	private static final String ZOOKEEPER_SERVERS_KEY = "ZookeeperServers";
	private static final String INSTANCE_NAME_KEY = "InstanceName";
	private static final String USERNAME_KEY = "UserName";
	private static final String PASSWORD_KEY = "Password";
	private static final String NAMESPACE_KEY = "Namespace";

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
	private static final Param NAMESPACE = new Param(
			NAMESPACE_KEY,
			String.class,
			"The table namespace associated with this data store",
			true);

	private final String zookeeperServers;
	private final String instanceName;
	private final String userName;
	private final String password;
	private final String namespace;

	private static List<Param> accumuloParams = null;

	public static List<Param> getPluginParams() {
		if (accumuloParams == null) {
			accumuloParams = new ArrayList<Param>();
			accumuloParams.add(ZOOKEEPER_SERVERS);
			accumuloParams.add(INSTANCE_NAME);
			accumuloParams.add(USERNAME);
			accumuloParams.add(PASSWORD);
			accumuloParams.add(NAMESPACE);
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

		param = params.get(NAMESPACE_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"Accumulo Plugin: Missing namespace param");
		}
		namespace = param.toString();
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

	public String getNamespace() {
		return namespace;
	}
}
