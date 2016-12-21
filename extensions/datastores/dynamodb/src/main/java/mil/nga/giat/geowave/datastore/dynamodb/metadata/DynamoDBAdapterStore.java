package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;

/**
 * This class will persist Data Adapters within an DynamoDB table for GeoWave
 * metadata. The adapters will be persisted in an "ADAPTER" column family.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 */
public class DynamoDBAdapterStore extends
		AbstractDynamoDBPersistence<DataAdapter<?>> implements
		AdapterStore
{
	private static final String ADAPTER_CF = "ADAPTER";

	public DynamoDBAdapterStore(
			final DynamoDBOperations dynamodbOperations ) {
		super(
				dynamodbOperations);
	}

	@Override
	public void addAdapter(
			final DataAdapter<?> adapter ) {
		addObject(adapter);
	}

	@Override
	public DataAdapter<?> getAdapter(
			final ByteArrayId adapterId ) {
		return getObject(
				adapterId,
				null);
	}

	@Override
	public boolean adapterExists(
			final ByteArrayId adapterId ) {
		return objectExists(
				adapterId,
				null);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final DataAdapter<?> persistedObject ) {
		return persistedObject.getAdapterId();
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return getObjects();
	}

	@Override
	protected String getPersistenceTypeName() {
		return ADAPTER_CF;
	}
}
