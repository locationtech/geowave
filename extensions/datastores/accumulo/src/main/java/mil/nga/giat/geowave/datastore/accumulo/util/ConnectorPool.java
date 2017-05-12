package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;

public class ConnectorPool
{
	private static ConnectorPool singletonInstance;

	public static synchronized ConnectorPool getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new ConnectorPool();
		}
		return singletonInstance;
	}

	private final Map<ConnectorConfig, Connector> connectorCache = new HashMap<ConnectorConfig, Connector>();

	public synchronized Connector getConnector(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password )
			throws AccumuloException,
			AccumuloSecurityException {

		final ConnectorConfig config = new ConnectorConfig(
				zookeeperUrl,
				instanceName,
				userName,
				password);
		Connector connector = connectorCache.get(config);
		if (connector == null) {
			final Instance inst = new ZooKeeperInstance(
					instanceName,
					zookeeperUrl);
			connector = inst.getConnector(
					userName,
					password);
			connectorCache.put(
					config,
					connector);
		}
		return connector;
	}

	private static class ConnectorConfig
	{
		private final String zookeeperUrl;
		private final String instanceName;
		private final String userName;
		private final String password;

		public ConnectorConfig(
				final String zookeeperUrl,
				final String instanceName,
				final String userName,
				final String password ) {
			this.zookeeperUrl = zookeeperUrl;
			this.instanceName = instanceName;
			this.userName = userName;
			this.password = password;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((instanceName == null) ? 0 : instanceName.hashCode());
			result = (prime * result) + ((password == null) ? 0 : password.hashCode());
			result = (prime * result) + ((userName == null) ? 0 : userName.hashCode());
			result = (prime * result) + ((zookeeperUrl == null) ? 0 : zookeeperUrl.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final ConnectorConfig other = (ConnectorConfig) obj;
			if (instanceName == null) {
				if (other.instanceName != null) {
					return false;
				}
			}
			else if (!instanceName.equals(other.instanceName)) {
				return false;
			}
			if (password == null) {
				if (other.password != null) {
					return false;
				}
			}
			else if (!password.equals(other.password)) {
				return false;
			}
			if (userName == null) {
				if (other.userName != null) {
					return false;
				}
			}
			else if (!userName.equals(other.userName)) {
				return false;
			}
			if (zookeeperUrl == null) {
				if (other.zookeeperUrl != null) {
					return false;
				}
			}
			else if (!zookeeperUrl.equals(other.zookeeperUrl)) {
				return false;
			}
			return true;
		}
	}
}
