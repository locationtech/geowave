package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow.CassandraField;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class CassandraReader implements
		Reader
{
	private final static Logger LOGGER = Logger.getLogger(CassandraReader.class);
	private static final boolean ASYNC = false;
	private final ReaderParams readerParams;
	private final RecordReaderParams recordReaderParams;
	private final CassandraOperations operations;
	private final boolean clientSideRowMerging;

	private final boolean wholeRowEncoding;
	private final int partitionKeyLength;
	private CloseableIterator<GeoWaveRow> iterator;

	public CassandraReader(
			final ReaderParams readerParams,
			final CassandraOperations operations ) {
		this.readerParams = readerParams;
		recordReaderParams = null;
		this.operations = operations;

		partitionKeyLength = readerParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		wholeRowEncoding = readerParams.isMixedVisibility() && !readerParams.isServersideAggregation();
		clientSideRowMerging = readerParams.isClientsideRowMerging();

		initScanner();
	}

	public CassandraReader(
			final RecordReaderParams recordReaderParams,
			final CassandraOperations operations ) {
		readerParams = null;
		this.recordReaderParams = recordReaderParams;
		this.operations = operations;

		partitionKeyLength = recordReaderParams.getIndex().getIndexStrategy().getPartitionKeyLength();
		wholeRowEncoding = recordReaderParams.isMixedVisibility() && !recordReaderParams.isServersideAggregation();
		clientSideRowMerging = false;

		initRecordScanner();
	}

	protected void initScanner() {
		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		if ((ranges != null) && !ranges.isEmpty()) {
			iterator = operations.getBatchedRangeRead(
					readerParams.getIndex().getId().getString(),
					readerParams.getAdapterIds(),
					ranges).results();
		}
		else {
			// TODO figure out the query select by adapter IDs here
			iterator = operations.executeQuery(operations.getSelect(readerParams.getIndex().getId().getString()));
		}
		// else if ((adapterIds != null) && !adapterIds.isEmpty()) {
		// //TODO this isn't going to work because there aren't partition keys
		// being passed along
		// requests.addAll(
		// getAdapterOnlyQueryRequests(
		// tableName,
		// adapterIds));
		// }

	}

	protected void initRecordScanner() {}

	@Override
	public void close()
			throws Exception {
		iterator.close();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public GeoWaveRow next() {
		return iterator.next();
	}

}
