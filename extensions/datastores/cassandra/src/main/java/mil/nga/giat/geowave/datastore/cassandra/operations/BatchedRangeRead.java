package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.EmptyIterator;

import com.datastax.driver.core.PreparedStatement;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.datastore.cassandra.CassandraIndexWriter;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;

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
		ranges.add(
				range);
	}

	public Iterator<CassandraRow> results() {
		for (int p = 0; p < CassandraIndexWriter.PARTITIONS; p++) {

		}
		return EmptyIterator.INSTANCE;
	}
}
