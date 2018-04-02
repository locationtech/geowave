package mil.nga.giat.geowave.datastore.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class AccumuloFactoryHelper implements
		StoreFactoryHelper
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloFactoryHelper.class);

	@Override
	public StoreFactoryOptions createOptionsInstance() {
		return new AccumuloRequiredOptions();
	}

	@Override
	public DataStoreOperations createOperations(
			final StoreFactoryOptions options ) {
		try {
			return AccumuloOperations.createOperations((AccumuloRequiredOptions) options);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to create Accumulo operations from config options",
					e);
			return null;
		}
	}

}
