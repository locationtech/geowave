package mil.nga.giat.geowave.datastore.cassandra.operations;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Deleter;

public class CassandraDeleter implements
		Deleter
{
	private final CassandraOperations operations;
	private final String tableName;

	public CassandraDeleter(
			final CassandraOperations operations,
			final String tableName ) {
		this.operations = operations;
		this.tableName = tableName;
	}

	@Override
	public void delete(
			final GeoWaveRow row,
			final DataAdapter<?> adapter ) {
		operations.deleteRow(
				tableName,
				row);
	}

	@Override
	public void close()
			throws Exception {

	}
}
