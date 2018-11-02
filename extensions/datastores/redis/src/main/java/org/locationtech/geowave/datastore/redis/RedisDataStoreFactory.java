package org.locationtech.geowave.datastore.redis;

import org.locationtech.geowave.core.store.BaseDataStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryHelper;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.redis.config.RedisOptions;
import org.locationtech.geowave.datastore.redis.operations.RedisOperations;

public class RedisDataStoreFactory extends
		BaseDataStoreFactory
{

	public RedisDataStoreFactory(
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
		if (!(options instanceof RedisOptions)) {
			throw new AssertionError(
					"Expected " + RedisOptions.class.getSimpleName());
		}

		return new RedisDataStore(
				(RedisOperations) helper.createOperations(options),
				((RedisOptions) options).getStoreOptions());
	}
}
