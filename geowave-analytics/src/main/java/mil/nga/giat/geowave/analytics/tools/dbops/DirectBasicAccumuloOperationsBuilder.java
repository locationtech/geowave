package mil.nga.giat.geowave.analytics.tools.dbops;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

public class DirectBasicAccumuloOperationsBuilder implements
		BasicAccumuloOperationsBuilder
{
	public static Map<String, BasicAccumuloOperations> connectionSet = new HashMap<String, BasicAccumuloOperations>();

	public BasicAccumuloOperations build(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace )
			throws AccumuloException,
			AccumuloSecurityException {
		final String key = buildKey(
				zookeeperUrl,
				instanceName,
				userName,
				password,
				tableNamespace);
		synchronized (connectionSet) {
			final boolean hasOps = connectionSet.containsKey(key);
			final BasicAccumuloOperations ops = hasOps ? connectionSet.get(key) : new BasicAccumuloOperations(
					zookeeperUrl,
					instanceName,
					userName,
					password,
					tableNamespace);
			if (!hasOps) connectionSet.put(
					key,
					ops);
			return ops;
		}
	}

	private static final String buildKey(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace ) {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(zookeeperUrl);
		buffer.append(instanceName);
		buffer.append(userName);
		buffer.append(password.hashCode());
		buffer.append(tableNamespace);
		return buffer.toString();

	}
}
