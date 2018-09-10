package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowMergingIterator;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import mil.nga.giat.geowave.core.store.filter.ClientVisibilityFilter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

public class CassandraReader<T> implements
		Reader<T>
{
	private final ReaderParams<T> readerParams;
	private final RecordReaderParams<T> recordReaderParams;
	private final CassandraOperations operations;
	private final GeoWaveRowIteratorTransformer<T> rowTransformer;

	private CloseableIterator<T> iterator;

	public CassandraReader(
			final ReaderParams<T> readerParams,
			final CassandraOperations operations ) {
		this.readerParams = readerParams;
		recordReaderParams = null;
		this.operations = operations;
		this.rowTransformer = readerParams.getRowTransformer();

		initScanner();
	}

	public CassandraReader(
			final RecordReaderParams<T> recordReaderParams,
			final CassandraOperations operations ) {
		readerParams = null;
		this.recordReaderParams = recordReaderParams;
		this.operations = operations;

		this.rowTransformer = recordReaderParams.getRowTransformer();

		initRecordScanner();
	}

	@SuppressWarnings("unchecked")
	private CloseableIterator<T> wrapResults(
			final CloseableIterator<CassandraRow> results,
			final Set<String> authorizations ) {
		return new CloseableIteratorWrapper<T>(
				results,
				rowTransformer
						.apply((Iterator<GeoWaveRow>) (Iterator<? extends GeoWaveRow>) new GeoWaveRowMergingIterator<CassandraRow>(
								Iterators.filter(
										results,
										readerParams.isAuthorizationsLimiting() ? new ClientVisibilityFilter(
												authorizations) : Predicates.alwaysTrue()))));
	}

	protected void initScanner() {
		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		final Set<String> authorizations = Sets.newHashSet(readerParams.getAdditionalAuthorizations());
		if ((ranges != null) && !ranges.isEmpty()) {
			iterator = operations.getBatchedRangeRead(
					readerParams.getIndex().getId().getString(),
					readerParams.getAdapterIds(),
					ranges,
					rowTransformer,
					readerParams.isAuthorizationsLimiting() ? new ClientVisibilityFilter(
							authorizations) : Predicates.alwaysTrue()).results();
		}
		else {
			// TODO figure out the query select by adapter IDs here
			final Select select = operations.getSelect(readerParams.getIndex().getId().getString());
			CloseableIterator<CassandraRow> results = operations.executeQuery(select);
			if ((readerParams.getAdapterIds() != null) && !readerParams.getAdapterIds().isEmpty()) {
				// TODO because we aren't filtering server-side by adapter ID,
				// we will need to filter here on the client
				results = new CloseableIteratorWrapper<CassandraRow>(
						results,
						Iterators.filter(
								results,
								new Predicate<CassandraRow>() {
									@Override
									public boolean apply(
											final CassandraRow input ) {
										return readerParams.getAdapterIds().contains(
												input.getInternalAdapterId());
									}
								}));
			}
			iterator = wrapResults(
					results,
					authorizations);
		}

	}

	protected void initRecordScanner() {
		final Collection<Short> adapterIds = recordReaderParams.getAdapterIds() != null ? recordReaderParams
				.getAdapterIds() : Lists.newArrayList();

		final GeoWaveRowRange range = recordReaderParams.getRowRange();
		final ByteArrayId startKey = range.isInfiniteStartSortKey() ? null : new ByteArrayId(
				range.getStartSortKey());
		final ByteArrayId stopKey = range.isInfiniteStopSortKey() ? null : new ByteArrayId(
				range.getEndSortKey());
		final SinglePartitionQueryRanges partitionRange = new SinglePartitionQueryRanges(
				new ByteArrayId(
						range.getPartitionKey()),
				Collections.singleton(new ByteArrayRange(
						startKey,
						stopKey)));
		final Set<String> authorizations = Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations());
		iterator = operations.getBatchedRangeRead(
				recordReaderParams.getIndex().getId().getString(),
				adapterIds,
				Collections.singleton(partitionRange),
				rowTransformer,
				readerParams.isAuthorizationsLimiting() ? new ClientVisibilityFilter(
						authorizations) : Predicates.alwaysTrue()).results();
	}

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
	public T next() {
		return iterator.next();
	}

}
