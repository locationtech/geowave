package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.operations.Deleter;

public class DynamoDBRowDeleter implements
		Deleter<DynamoDBRow>
{
	private final String tableName;
	private final DynamoDBOperations operations;
	private final String[] additionalAuthorizations;

	public DynamoDBRowDeleter(
			final DynamoDBOperations operations,
			final String tableName,
			final String[] additionalAuthorizations ) {
		this.operations = operations;
		this.tableName = tableName;
		this.additionalAuthorizations = additionalAuthorizations;
	}

	@Override
	public void close()
			throws IOException {}

	@Override
	public void delete(
			final DataStoreEntryInfo entry,
			final DynamoDBRow nativeRow,
			final DataAdapter<?> adapter ) {
		operations.deleteRow(
				tableName,
				nativeRow,
				additionalAuthorizations);
	}
}