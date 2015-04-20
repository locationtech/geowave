package mil.nga.giat.geowave.analytics.tools.dbops;

import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.analytics.parameters.CommonParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.store.index.IndexStore;

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
