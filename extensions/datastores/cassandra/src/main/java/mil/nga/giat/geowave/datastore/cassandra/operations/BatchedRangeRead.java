package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.beust.jcommander.internal.Lists;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.TypeCodec;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;

public class BatchedRangeRead
{
	private final CassandraOperations operations;
	private final PreparedStatement preparedRead;
	private final Collection<SinglePartitionQueryRanges> ranges;
	private final Collection<Short> internalAdapterIds;

	protected BatchedRangeRead(
			final PreparedStatement preparedRead,
			final CassandraOperations operations,
			final Collection<Short> internalAdapterIds,
			final Collection<SinglePartitionQueryRanges> ranges ) {
		this.preparedRead = preparedRead;
		this.operations = operations;
		this.internalAdapterIds = internalAdapterIds;
		this.ranges = ranges;
	}

	public CloseableIterator<CassandraRow> results() {
		final List<BoundStatement> statements = new ArrayList<>();
		for (final SinglePartitionQueryRanges r : ranges) {
			for (final ByteArrayRange range : r.getSortKeyRanges()) {
				final BoundStatement boundRead = new BoundStatement(
						preparedRead);
				final byte[] start = range.getStart() != null ? range.getStart().getBytes() : new byte[0];
				final byte[] end = range.getEnd() != null ? range.getEndAsNextPrefix().getBytes() : new byte[] {
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF
				};
				boundRead.set(
						CassandraField.GW_SORT_KEY.getLowerBoundBindMarkerName(),
						ByteBuffer.wrap(start),
						ByteBuffer.class);

				boundRead.set(
						CassandraField.GW_SORT_KEY.getUpperBoundBindMarkerName(),
						ByteBuffer.wrap(end),
						ByteBuffer.class);
				boundRead.set(
						CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
						ByteBuffer.wrap(r.getPartitionKey().getBytes()),
						ByteBuffer.class);

				boundRead.set(
						CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
						Lists.newArrayList(internalAdapterIds),
						TypeCodec.list(TypeCodec.smallInt()));
				statements.add(boundRead);
			}

		}
		return operations.executeQueryAsync(statements.toArray(new BoundStatement[] {}));
	}
}
