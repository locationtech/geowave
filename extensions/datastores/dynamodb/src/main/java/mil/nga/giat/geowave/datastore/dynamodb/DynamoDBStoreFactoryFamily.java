package mil.nga.giat.geowave.datastore.dynamodb;

import mil.nga.giat.geowave.core.store.BaseDataStoreFamily;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class DynamoDBStoreFactoryFamily extends
		BaseDataStoreFamily
{
	public final static String TYPE = "dynamodb";
	private static final String DESCRIPTION = "A GeoWave store backed by tables in DynamoDB";

	public DynamoDBStoreFactoryFamily() {
		super(
				TYPE,
				DESCRIPTION,
				new DynamoDBFactoryHelper());
	}

	@Override
	public GenericStoreFactory<DataStore> getDataStoreFactory() {
		return new DynamoDBDataStoreFactory(
				TYPE,
				DESCRIPTION,
				new DynamoDBFactoryHelper());
	}
}
