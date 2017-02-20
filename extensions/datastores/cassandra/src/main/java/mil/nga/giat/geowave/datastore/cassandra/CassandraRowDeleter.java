package mil.nga.giat.geowave.datastore.cassandra;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraRowDeleter implements
		Deleter<CassandraRow>
{
	private final String tableName;
	private final CassandraOperations operations;
	private final String[] additionalAuthorizations;

	public CassandraRowDeleter(
			final CassandraOperations operations,
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
			final CassandraRow nativeRow,
			final DataAdapter<?> adapter ) {
		operations.deleteRow(
				tableName,
				nativeRow,
				additionalAuthorizations);
	}
}
