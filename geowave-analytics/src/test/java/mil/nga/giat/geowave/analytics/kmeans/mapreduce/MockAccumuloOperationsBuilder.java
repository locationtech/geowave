package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.analytics.tools.dbops.BasicAccumuloOperationsBuilder;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public class MockAccumuloOperationsBuilder implements
		BasicAccumuloOperationsBuilder
{
	final static MockInstance mockDataInstance = new MockInstance();
	static Connector mockDataConnector = null;

	public MockAccumuloOperationsBuilder() {
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
