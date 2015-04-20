package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

public class DirectBasicAccumuloOperationsFactory implements
		BasicAccumuloOperationsFactory
{
	@Override
	public BasicAccumuloOperations build(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace )
			throws AccumuloException,
			AccumuloSecurityException {
		// BasicAccumuloOperations has a built in connection pool
		return new BasicAccumuloOperations(
				zookeeperUrl,
				instanceName,
				userName,
				password,
				tableNamespace);

	}

}
