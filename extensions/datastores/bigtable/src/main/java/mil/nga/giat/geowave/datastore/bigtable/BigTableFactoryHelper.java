package mil.nga.giat.geowave.datastore.bigtable;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.BigTableOperations;
import mil.nga.giat.geowave.datastore.bigtable.operations.config.BigTableOptions;

public class BigTableFactoryHelper implements
		StoreFactoryHelper
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BigTableFactoryHelper.class);

	@Override
	public StoreFactoryOptions createOptionsInstance() {
		return new BigTableOptions();
	}

	@Override
	public DataStoreOperations createOperations(
			final StoreFactoryOptions options ) {
		try {
			return BigTableOperations.createOperations((BigTableOptions) options);
		}
		catch (IOException e) {
			LOGGER.error(
					"Unable to create BigTable operations from config options",
					e);
			return null;
		}
	}

}
