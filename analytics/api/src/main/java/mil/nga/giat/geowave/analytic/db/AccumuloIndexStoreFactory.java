package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloIndexStoreFactory implements
		IndexStoreFactory
{
	final static Logger LOGGER = LoggerFactory.getLogger(AccumuloIndexStoreFactory.class);

	@Override
	public IndexStore getIndexStore(
			ConfigurationWrapper context )
			throws InstantiationException {

		final String zookeeper = context.getString(
				GlobalParameters.Global.ZOOKEEKER,
				this.getClass(),
				"localhost:2181");
		final String accumuloInstance = context.getString(
				GlobalParameters.Global.ACCUMULO_INSTANCE,
				this.getClass(),
				"minInstance");

		BasicAccumuloOperations basicAccumuloOperations;
		try {
			basicAccumuloOperations = context.getInstance(
					CommonParameters.Common.ACCUMULO_CONNECT_FACTORY,
					this.getClass(),
					BasicAccumuloOperationsFactory.class,
					DirectBasicAccumuloOperationsFactory.class).build(
					zookeeper,
					accumuloInstance,
					context.getString(
							GlobalParameters.Global.ACCUMULO_USER,
							this.getClass(),
							"root"),
					context.getString(
							GlobalParameters.Global.ACCUMULO_PASSWORD,
							this.getClass(),
							""),
					context.getString(
							GlobalParameters.Global.ACCUMULO_NAMESPACE,
							this.getClass(),
							""));
		}
		catch (IllegalAccessException | AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Cannot connect to GeoWave for Adapter Inquiry (" + accumuloInstance + "@ " + zookeeper + ")",
					e);
			throw new InstantiationException(
					"Cannot connect to GeoWave for Adapter Inquiry (" + accumuloInstance + "@ " + zookeeper + ")");
		}
		return new AccumuloIndexStore(
				basicAccumuloOperations);

	}

}
