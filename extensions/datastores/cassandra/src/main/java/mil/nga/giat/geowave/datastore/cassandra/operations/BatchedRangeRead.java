package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.cassandra.CassandraIndexWriter;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;

public class BatchedRangeRead
{
	private final CassandraOperations operations;
	private final PreparedStatement preparedRead;
	private final List<ByteArrayRange> ranges;

	protected BatchedRangeRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations ) {
		this(
				preparedRead,
				operations,
				new ArrayList<>());
	}

	protected BatchedRangeRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations,
			final List<ByteArrayRange> ranges ) {
		this.preparedRead = preparedRead;
		this.operations = operations;
		this.ranges = ranges;
	}

	public void addQueryRange(
			final ByteArrayRange range ) {
		ranges.add(range);
	}

	public CloseableIterator<CassandraRow> results() {
		final List<BoundStatement> statements = new ArrayList<>();
		for (int p = 0; p < CassandraIndexWriter.PARTITIONS; p++) {
			for (final ByteArrayRange range : ranges) {
				final BoundStatement boundRead = new BoundStatement(
						preparedRead);
				boundRead.set(
						CassandraField.GW_IDX_KEY.getLowerBoundBindMarkerName(),
						ByteBuffer.wrap(range.getStart().getBytes()),
						ByteBuffer.class);

				boundRead.set(
						CassandraField.GW_IDX_KEY.getUpperBoundBindMarkerName(),
						ByteBuffer.wrap(range.getEndAsNextPrefix().getBytes()),
						ByteBuffer.class);
				boundRead.set(
						CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
						ByteBuffer.wrap(new byte[] {
							(byte) p
						}),
						ByteBuffer.class);
				statements.add(boundRead);
			}

		}
		return operations.executeQuery(statements.toArray(new BoundStatement[] {}));
	}
}
