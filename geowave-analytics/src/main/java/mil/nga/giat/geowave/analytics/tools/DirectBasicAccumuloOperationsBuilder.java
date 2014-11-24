package mil.nga.giat.geowave.analytics.tools;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

public class DirectBasicAccumuloOperationsBuilder implements
		BasicAccumuloOperationsBuilder
{
	public BasicAccumuloOperations build(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace )
			throws AccumuloException,
			AccumuloSecurityException {
		return new BasicAccumuloOperations(
				zookeeperUrl,
				instanceName,
				userName,
				password,
				tableNamespace);
	}
}
