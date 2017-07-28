package mil.nga.giat.geowave.test;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBStoreFactoryFamily;
//import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBDataStoreFactory;
//import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class DynamoDBTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new DynamoDBStoreFactoryFamily()
			.getDataStoreFactory();

	private static DynamoDBTestEnvironment singletonInstance = null;

	public static synchronized DynamoDBTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new DynamoDBTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = Logger.getLogger(DynamoDBTestEnvironment.class);

	protected DynamoDBLocal dynamoLocal;

	private DynamoDBTestEnvironment() {}

	@Override
	public void setup() {
		// DynamoDB IT's rely on an external dynamo local process
		if (dynamoLocal == null) {
			dynamoLocal = new DynamoDBLocal(
					null); // null uses tmp dir
		}

		// Make sure we clean up any old processes first
		if (dynamoLocal.isRunning()) {
			dynamoLocal.stop();
		}

		if (!dynamoLocal.start()) {
			LOGGER.error("DynamoDB emulator startup failed");
		}
	}

	@Override
	public void tearDown() {
		dynamoLocal.stop();
	}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.DYNAMODB;
	}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {
		((DynamoDBOptions) options).setEndpoint("http://localhost:8000");
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

}
