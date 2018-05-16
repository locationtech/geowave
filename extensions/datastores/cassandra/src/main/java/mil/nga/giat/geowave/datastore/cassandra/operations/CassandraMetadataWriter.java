package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;

public class CassandraMetadataWriter implements
		MetadataWriter
{
	protected static final String PRIMARY_ID_KEY = "I";
	protected static final String SECONDARY_ID_KEY = "S";
	// serves as unique ID for instances where primary+secondary are repeated
	protected static final String TIMESTAMP_ID_KEY = "T";
	protected static final String VALUE_KEY = "V";

	private final CassandraOperations operations;
	private final String tableName;

	public CassandraMetadataWriter(
			final CassandraOperations operations,
			final String tableName ) {
		this.operations = operations;
		this.tableName = tableName;
	}

	@Override
	public void close()
			throws Exception {

	}

	@Override
	public void write(
			final GeoWaveMetadata metadata ) {
		final Insert insert = operations.getInsert(tableName);
		insert.value(
				PRIMARY_ID_KEY,
				ByteBuffer.wrap(metadata.getPrimaryId()));
		if (metadata.getSecondaryId() != null) {
			insert.value(
					SECONDARY_ID_KEY,
					ByteBuffer.wrap(metadata.getSecondaryId()));
			insert.value(
					TIMESTAMP_ID_KEY,
					QueryBuilder.now());
		}
		insert.value(
				VALUE_KEY,
				ByteBuffer.wrap(metadata.getValue()));
		operations.getSession().execute(
				insert);
	}

	@Override
	public void flush() {

	}

}
