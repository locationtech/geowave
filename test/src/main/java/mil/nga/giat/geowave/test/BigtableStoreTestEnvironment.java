package mil.nga.giat.geowave.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.bigtable.BigTableDataStoreFactory;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class BigtableStoreTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new BigTableDataStoreFactory();
	private static BigtableStoreTestEnvironment singletonInstance = null;

	public static synchronized BigtableStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new BigtableStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = LoggerFactory.getLogger(BigtableStoreTestEnvironment.class);

	protected BigtableEmulator emulator;

	private BigtableStoreTestEnvironment() {}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.BIGTABLE;
	}

	@Override
	public void setup() {
		// Bigtable IT's rely on an external gcloud emulator
		EnvironmentVariables environmentVariables = new EnvironmentVariables();
		environmentVariables.set(
				"BIGTABLE_EMULATOR_HOST",
				"localhost:8128");
		if (emulator == null) {
			emulator = new BigtableEmulator(
					null); // null uses tmp dir
		}

		// Make sure we clean up any old processes first
		if (emulator.isRunning()) {
			emulator.stop();
		}

		if (!emulator.start()) {
			LOGGER.error("Bigtable emulator startup failed");
		}
	}

	@Override
	public void tearDown() {
		emulator.stop();
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}
}
