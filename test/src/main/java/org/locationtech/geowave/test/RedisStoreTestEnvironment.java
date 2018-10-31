package org.locationtech.geowave.test;

import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.redis.RedisStoreFactoryFamily;
import org.locationtech.geowave.datastore.redis.config.RedisOptions;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

import redis.embedded.RedisServer;

public class RedisStoreTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new RedisStoreFactoryFamily()
			.getDataStoreFactory();

	private static RedisStoreTestEnvironment singletonInstance = null;

	private RedisServer redisServer;

	public static synchronized RedisStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new RedisStoreTestEnvironment();
		}
		return singletonInstance;
	}

	@Override
	public void setup() {
		if (redisServer == null) {
			redisServer = RedisServer.builder().port(
					6379).setting(
					"bind 127.0.0.1") // secure + prevents popups on Windows
					.setting(
							"maxmemory 512M")
					.setting(
							"timeout 30000")
					.build();
			redisServer.start();
		}

	}

	@Override
	public void tearDown() {
		if (redisServer != null) {
			redisServer.stop();
			redisServer = null;
		}
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.REDIS;
	}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		((RedisOptions) options).setAddress("redis://127.0.0.1:6379");
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}
}
