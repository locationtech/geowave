package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.cassandra.CassandraIndexWriter;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;

public class RowRead
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			RowRead.class);
	private final CassandraOperations operations;
	private final PreparedStatement preparedRead;
	private byte[] row;

	protected RowRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations ) {
		this(
				preparedRead,
				operations,
				null);
	}

	protected RowRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations,
			final byte[] row ) {
		this.preparedRead = preparedRead;
		this.operations = operations;
		this.row = row;
	}

	public void setRow(
			final byte[] row ) {
		this.row = row;
	}

	public CassandraRow result() {
		if (row != null) {
			final Statement[] statements = new Statement[CassandraIndexWriter.PARTITIONS];
			for (int p = 0; p < CassandraIndexWriter.PARTITIONS; p++) {
				final BoundStatement boundRead = new BoundStatement(
						preparedRead);
				boundRead.set(
						CassandraField.GW_IDX_KEY.getBindMarkerName(),
						ByteBuffer.wrap(
								row),
						ByteBuffer.class);
				statements[p] = boundRead;
			}
			try (CloseableIterator<CassandraRow> it = operations.executeQuery(
					statements)) {
				if (it.hasNext()) {
					// there should only be one entry with this index
					return it.next();
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to close cassandra query",
						e);
			}
		}
		return null;
	}
}
