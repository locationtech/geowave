package mil.nga.giat.geowave.datastore.dynamodb.index.secondary;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;

// TODO implement secondary indexing for DynamoDB
public class DynamoDBSecondaryIndexDataStore implements
		SecondaryIndexDataStore
{
	private final DynamoDBOperations operations;

	public DynamoDBSecondaryIndexDataStore(
			final DynamoDBOperations operations ) {
		this.operations = operations;
	}

	@Override
	public void setDataStore(
			final DataStore dataStore ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void storeJoinEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId primaryIndexId,
			final ByteArrayId primaryIndexRowId,
			final ByteArrayId attributeVisibility ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void storeEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId dataId,
			final ByteArrayId attributeVisibility,
			final List<FieldInfo<?>> attributes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> CloseableIterator<T> query(
			final SecondaryIndex<T> secondaryIndex,
			final ByteArrayId indexedAttributeFieldId,
			final DataAdapter<T> adapter,
			final PrimaryIndex primaryIndex,
			final DistributableQuery query,
			final String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteJoinEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId primaryIndexId,
			final ByteArrayId primaryIndexRowId ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteEntry(
			final ByteArrayId secondaryIndexId,
			final ByteArrayId indexedAttributeValue,
			final ByteArrayId adapterId,
			final ByteArrayId indexedAttributeFieldId,
			final ByteArrayId dataId,
			final List<FieldInfo<?>> attributes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeAll() {
		// TODO Auto-generated method stub

	}

}
