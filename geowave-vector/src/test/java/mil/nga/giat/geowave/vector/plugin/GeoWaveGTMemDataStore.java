package mil.nga.giat.geowave.vector.plugin;

import java.net.URL;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.vector.AccumuloDataStatisticsStoreExt;
import mil.nga.giat.geowave.vector.VectorDataStore;
import mil.nga.giat.geowave.vector.auth.AuthorizationFactorySPI;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

/**
 * For unit testing
 */
public class GeoWaveGTMemDataStore extends
		GeoWaveGTDataStore
{

	public GeoWaveGTMemDataStore(
			final String instanceName )
			throws AccumuloException,
			AccumuloSecurityException {
		super(
				new MemoryTransactionsAllocater());
		((MemoryTransactionsAllocater) super.getTransactionsAllocater()).setNotificationRequester(this);
		init(instanceName);

	}

	public GeoWaveGTMemDataStore()
			throws AccumuloException,
			AccumuloSecurityException {
		super(
				new MemoryTransactionsAllocater());
		((MemoryTransactionsAllocater) super.getTransactionsAllocater()).setNotificationRequester(this);
		init(UUID.randomUUID().toString());

	}

	public GeoWaveGTMemDataStore(
			final AuthorizationFactorySPI authorizationFactorySPI,
			final URL authURL,
			final String instanceName )
			throws AccumuloException,
			AccumuloSecurityException {
		super(
				new MemoryTransactionsAllocater(),
				authorizationFactorySPI.create(authURL));
		((MemoryTransactionsAllocater) super.getTransactionsAllocater()).setNotificationRequester(this);
		init(instanceName);
	}

	public void init(
			final String instanceName )
			throws AccumuloException,
			AccumuloSecurityException {

		final MockInstance mockDataInstance = new MockInstance(
				instanceName);
		final Connector mockDataConnector = mockDataInstance.getConnector(
				"root",
				new PasswordToken(
						new byte[0]));

		final BasicAccumuloOperations dataOps = new BasicAccumuloOperations(
				mockDataConnector);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				dataOps);

		final DataStatisticsStore statisticsStore = new AccumuloDataStatisticsStoreExt(
				dataOps);

		super.setAdapterStore(new AccumuloAdapterStore(
				dataOps));
		super.setDataStore(new VectorDataStore(
				indexStore,
				super.getAdapterStore(),
				statisticsStore,
				dataOps));

		super.setAdapterStore(new AccumuloAdapterStore(
				dataOps));
		super.setDataStore(new VectorDataStore(
				indexStore,
				super.getAdapterStore(),
				statisticsStore,
				dataOps));

		super.setStatsOperations(dataOps);
		super.setStatsDataStore(new VectorDataStore(
				indexStore,
				super.getAdapterStore(),
				statisticsStore,
				dataOps));
		super.setStoreOperations(dataOps);

	}
}
