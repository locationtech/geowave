package org.locationtech.geowave.datastore.rocksdb;

import org.locationtech.geowave.core.store.BaseDataStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.locationtech.geowave.datastore.rocksdb.operations.RocksDBOperations;

public class RocksDBDataStoreFactory extends
		BaseDataStoreFactory
{

	public RocksDBDataStoreFactory(
			final String typeName,
			final String description,
			final StoreFactoryHelper helper ) {
		super(
				typeName,
				description,
				helper);
	}

	@Override
	public DataStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof RocksDBOptions)) {
			throw new AssertionError(
					"Expected " + RocksDBOptions.class.getSimpleName());
		}

		return new RocksDBDataStore(
				(RocksDBOperations) helper.createOperations(options),
				((RocksDBOptions) options).getStoreOptions());
	}
}
