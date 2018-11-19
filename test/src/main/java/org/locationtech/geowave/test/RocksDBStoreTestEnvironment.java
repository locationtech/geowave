package org.locationtech.geowave.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.locationtech.geowave.core.store.GenericStoreFactory;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.datastore.rocksdb.RocksDBStoreFactoryFamily;
import org.locationtech.geowave.datastore.rocksdb.config.RocksDBOptions;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClientCache;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class RocksDBStoreTestEnvironment extends
		StoreTestEnvironment
{

	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new RocksDBStoreFactoryFamily()
			.getDataStoreFactory();
	private static RocksDBStoreTestEnvironment singletonInstance = null;
	private static final String DEFAULT_DB_DIRECTORY = "./target/rocksdb";

	public static synchronized RocksDBStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new RocksDBStoreTestEnvironment();
		}
		return singletonInstance;
	}

	@Override
	public void setup()
			throws Exception {}

	@Override
	public void tearDown()
			throws Exception {
		// this helps clean up any outstanding native resources
		RocksDBClientCache.getInstance().closeAll();

		FileUtils.deleteDirectory(new File(
				DEFAULT_DB_DIRECTORY));
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.ROCKSDB;
	}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		((RocksDBOptions) options).setDirectory(DEFAULT_DB_DIRECTORY);
	}

}
