package org.locationtech.geowave.datastore.rocksdb.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.operations.BaseReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBClient;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

public class RocksDBReader<T> implements
		RowReader<T>
{
	private final CloseableIterator<T> iterator;

	public RocksDBReader(
			final RocksDBClient client,
			final ReaderParams<T> readerParams,
			final boolean async ) {
		this.iterator = createIteratorForReader(
				client,
				readerParams,
				false);
	}

	public RocksDBReader(
			final RocksDBClient client,
			final RecordReaderParams<T> recordReaderParams ) {
		this.iterator = createIteratorForRecordReader(
				client,
				recordReaderParams);
	}

	private CloseableIterator<T> createIteratorForReader(
			final RocksDBClient client,
			final ReaderParams<T> readerParams,
			final boolean async ) {
		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		final Set<String> authorizations = Sets
				.newHashSet(
						readerParams.getAdditionalAuthorizations());
		if ((ranges != null) && !ranges.isEmpty()) {
			return createIterator(
					client,
					readerParams,
					ranges,
					authorizations,
					async);
		}
		else {
			final List<CloseableIterator<GeoWaveRow>> iterators = new ArrayList<>();
			for (final short adapterId : readerParams.getAdapterIds()) {
				final Pair<Boolean, Boolean> groupByRowAndSortByTime = RocksDBUtils
						.isGroupByRowAndIsSortByTime(
								readerParams,
								adapterId);
				final String indexNamePrefix = RocksDBUtils
						.getTablePrefix(
								readerParams
										.getInternalAdapterStore()
										.getTypeName(
												adapterId),
								readerParams.getIndex().getName());
				final Stream<CloseableIterator<GeoWaveRow>> streamIt = RocksDBUtils
						.getPartitions(
								client.getSubDirectory(),
								indexNamePrefix)
						.stream()
						.map(
								p -> {
									return RocksDBUtils
											.getIndexTableFromPrefix(
													client,
													indexNamePrefix,
													adapterId,
													p.getBytes(),
													groupByRowAndSortByTime.getRight())
											.iterator();
								});
				iterators
						.addAll(
								streamIt
										.collect(
												Collectors.toList()));
			}
			return wrapResults(
					new Closeable() {
						AtomicBoolean closed = new AtomicBoolean(
								false);

						@Override
						public void close()
								throws IOException {
							if (!closed
									.getAndSet(
											true)) {
								iterators
										.forEach(
												it -> it.close());
							}
						}
					},
					Iterators
							.concat(
									iterators.iterator()),
					readerParams,
					authorizations);
		}
	}

	private CloseableIterator<T> createIterator(
			final RocksDBClient client,
			final BaseReaderParams<T> readerParams,
			final Collection<SinglePartitionQueryRanges> ranges,
			final Set<String> authorizations,
			final boolean async ) {
		final Iterator<CloseableIterator> it = Arrays
				.stream(
						ArrayUtils
								.toObject(
										readerParams.getAdapterIds()))
				.map(
						adapterId -> new BatchedRangeRead(
								client,
								RocksDBUtils
										.getTablePrefix(
												readerParams
														.getInternalAdapterStore()
														.getTypeName(
																adapterId),
												readerParams.getIndex().getName()),
								adapterId,
								ranges,
								readerParams.getRowTransformer(),
								new ClientVisibilityFilter(
										authorizations),
								async,
								RocksDBUtils
										.isGroupByRowAndIsSortByTime(
												readerParams,
												adapterId),
								RocksDBUtils
										.isSortByKeyRequired(
												readerParams)).results())
				.iterator();
		final CloseableIterator<T>[] itArray = Iterators
				.toArray(
						it,
						CloseableIterator.class);
		return new CloseableIteratorWrapper<>(
				new Closeable() {
					AtomicBoolean closed = new AtomicBoolean(
							false);

					@Override
					public void close()
							throws IOException {
						if (!closed
								.getAndSet(
										true)) {
							Arrays
									.stream(
											itArray)
									.forEach(
											it -> it.close());
						}
					}
				},
				Iterators
						.concat(
								itArray));
	}

	private CloseableIterator<T> createIteratorForRecordReader(
			final RocksDBClient client,
			final RecordReaderParams<T> recordReaderParams ) {
		final GeoWaveRowRange range = recordReaderParams.getRowRange();
		final ByteArray startKey = range.isInfiniteStartSortKey() ? null : new ByteArray(
				range.getStartSortKey());
		final ByteArray stopKey = range.isInfiniteStopSortKey() ? null : new ByteArray(
				range.getEndSortKey());
		final SinglePartitionQueryRanges partitionRange = new SinglePartitionQueryRanges(
				new ByteArray(
						range.getPartitionKey()),
				Collections.singleton(new ByteArrayRange(
						startKey,
						stopKey)));
		final Set<String> authorizations = Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations());
		return createIterator(
				client,
				recordReaderParams,
				Collections.singleton(partitionRange),
				authorizations,
				// there should already be sufficient parallelism created by
				// input splits for record reader use cases
				false);
	}

	@SuppressWarnings("unchecked")
	private CloseableIterator<T> wrapResults(
			final Closeable closeable,
			final Iterator<GeoWaveRow> results,
			final BaseReaderParams<T> params,
			final Set<String> authorizations ) {
		return new CloseableIteratorWrapper<>(
				closeable,
				params.getRowTransformer().apply(
						sortBySortKeyIfRequired(
								params,
								(Iterator<GeoWaveRow>) (Iterator<? extends GeoWaveRow>) new GeoWaveRowMergingIterator(
										Iterators.filter(
												results,
												new ClientVisibilityFilter(
														authorizations))))));
	}

	private static Iterator<GeoWaveRow> sortBySortKeyIfRequired(
			final BaseReaderParams<?> params,
			final Iterator<GeoWaveRow> it ) {
		if (RocksDBUtils.isSortByKeyRequired(params)) {
			return RocksDBUtils.sortBySortKey(it);
		}
		return it;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		return iterator.next();
	}

	@Override
	public void close()
			throws Exception {
		iterator.close();
	}
}
