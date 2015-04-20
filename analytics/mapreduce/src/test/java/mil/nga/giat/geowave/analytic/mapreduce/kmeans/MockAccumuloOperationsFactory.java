package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import mil.nga.giat.geowave.analytic.db.BasicAccumuloOperationsFactory;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public class MockAccumuloOperationsFactory implements
		BasicAccumuloOperationsFactory
{
	final static MockInstance mockDataInstance = new MockInstance();
	static Connector mockDataConnector = null;

	public MockAccumuloOperationsFactory() {
		synchronized (mockDataInstance) {
			if (mockDataConnector == null) {
				try {
					mockDataConnector = mockDataInstance.getConnector(
							"root",
							new PasswordToken(
									new byte[0]));
				}
				catch (AccumuloException e) {
					e.printStackTrace();
				}
				catch (AccumuloSecurityException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public BasicAccumuloOperations build(
			String zookeeperUrl,
			String instanceName,
			String userName,
			String password,
			String tableNamespace )
			throws AccumuloException,
			AccumuloSecurityException {
		return new BasicAccumuloOperations(
				mockDataConnector);
	}

}
