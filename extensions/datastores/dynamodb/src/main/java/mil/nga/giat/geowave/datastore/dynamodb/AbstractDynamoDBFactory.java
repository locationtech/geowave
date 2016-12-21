package mil.nga.giat.geowave.datastore.dynamodb;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

abstract public class AbstractDynamoDBFactory implements
		GenericFactory
{
	private static final String NAME = DynamoDBDataStore.TYPE;
	private static final String DESCRIPTION = "A GeoWave store backed by tables in Amazon DynamoDB";

	@Override
	public String getType() {
		return NAME;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

	/**
	 * This helps implementation of child classes by returning the default
	 * DynamoDB options that are required.
	 *
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new DynamoDBOptions();
	}
}
