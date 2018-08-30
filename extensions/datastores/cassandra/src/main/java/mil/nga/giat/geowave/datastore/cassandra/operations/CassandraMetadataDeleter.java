package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

public class CassandraMetadataDeleter implements
		MetadataDeleter
{
	private final CassandraOperations operations;
	private final MetadataType metadataType;

	public CassandraMetadataDeleter(
			final CassandraOperations operations,
			final MetadataType metadataType ) {
		this.operations = operations;
		this.metadataType = metadataType;
	}

	@Override
	public void close()
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean delete(
			final MetadataQuery query ) {
		final Delete delete = operations.getDelete(operations.getMetadataTableName(metadataType));
		if (query.hasPrimaryId()) {
			final Where where = delete.where(QueryBuilder.eq(
					CassandraMetadataWriter.PRIMARY_ID_KEY,
					ByteBuffer.wrap(query.getPrimaryId())));
			if (query.hasSecondaryId()) {
				where.and(QueryBuilder.eq(
						CassandraMetadataWriter.SECONDARY_ID_KEY,
						ByteBuffer.wrap(query.getSecondaryId())));
			}
		}
		// deleting by secondary ID without primary ID is not supported (and not
		// directly supported by cassandra, we'd have top query first to get
		// primary ID(s) and then delete, but this is not a use case necessary
		// at the moment
		operations.getSession().execute(
				delete);
		return true;
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

}
