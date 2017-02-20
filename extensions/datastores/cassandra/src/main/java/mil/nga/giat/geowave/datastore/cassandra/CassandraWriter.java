package mil.nga.giat.geowave.datastore.cassandra;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.datastore.cassandra.operations.BatchedWrite;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraWriter implements
		Writer<CassandraRow>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraWriter.class);
	private final Object MUTEX = new Object();
	private BatchedWrite batchedWrite = null;
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
			write(row);
		}
	}

	@Override
	public void write(
			final CassandraRow row ) {
		synchronized (MUTEX) {
			if (batchedWrite == null) {
				batchedWrite = operations.getBatchedWrite(tableName);
			}
			batchedWrite.insert(row);
		}
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
