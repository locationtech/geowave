package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;

public class DynamoDBIndexStore extends
		AbstractDynamoDBPersistence<Index<?, ?>> implements
		IndexStore
{
	private static final String INDEX_CF = "IDX";

	public DynamoDBIndexStore(
			final DynamoDBOperations ops ) {
		super(
				ops);
	}

	@Override
	public void addIndex(
			final Index<?, ?> index ) {
		addObject(index);
	}

	@Override
	public Index<?, ?> getIndex(
			final ByteArrayId indexId ) {
		return getObject(
				indexId,
				null);
	}

	@Override
	protected String getPersistenceTypeName() {
		return INDEX_CF;
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final Index<?, ?> persistedObject ) {
		return persistedObject.getId();
	}

	@Override
	public boolean indexExists(
			final ByteArrayId id ) {
		return objectExists(
				id,
				null);
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices() {
		return getObjects();
	}

}
