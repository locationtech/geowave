package mil.nga.giat.geowave.datastore.cassandra;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;
import mil.nga.giat.geowave.datastore.cassandra.operations.BatchedWrite;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraWriter implements
		Writer<CassandraRow>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(
			CassandraWriter.class);
	private final Object MUTEX = new Object();
	private PreparedStatement preparedInsert;
	private BatchedWrite batchedWrite;
	private final CassandraOperations operations;
	private final String tableName;

	public CassandraWriter(
			final String tableName,
			final CassandraOperations operations ) {
		this.tableName = tableName;
		this.operations = operations;
	}

	@Override
	public void close()
			throws IOException {
		flush();
	}

	@Override
	public void write(
			final Iterable<CassandraRow> rows ) {
		for (final CassandraRow row : rows) {
			write(
					row);
		}
	}

	@Override
	public void write(
			final CassandraRow row ) {
		synchronized (MUTEX) {
			if (preparedInsert == null) {
				preparedInsert = operations.getSession().prepare(
						getUnboundInsert());

				batchedWrite = operations.getBatchedWrite();
			}
			batchedWrite.insert(
					row.bindInsertion(
							preparedInsert));
		}
	}

	private Insert getUnboundInsert() {
		final Insert insert = operations.getInsert(
				tableName);
		for (final CassandraField f : CassandraField.values()) {
			insert.value(
					f.getFieldName(),
					QueryBuilder.bindMarker(
							f.getBindMarkerName()));
		}
		return insert;
	}

	@Override
	public void flush() {
		synchronized (MUTEX) {
			if (batchedWrite != null) {
				try {
					batchedWrite.close();
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to close batched write",
							e);
				}
			}
		}
	}
}
