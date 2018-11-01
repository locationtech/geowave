package org.locationtech.geowave.datastore.redis.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisRow;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

public class RedisReader<T> implements
		RowReader<T>
{
	private final CloseableIterator<T> iterator;

	public RedisReader(
			final RedissonClient client,
			final ReaderParams<T> readerParams,
			final String namespace,
			final boolean async ) {
		this.iterator = createIteratorForReader(
				client,
				readerParams,
				namespace,
				async);
	}

	public RedisReader(
			final RedissonClient client,
			final RecordReaderParams<T> recordReaderParams,
			final String namespace ) {
		this.iterator = createIteratorForRecordReader(
				client,
				recordReaderParams,
				namespace);
	}

	private CloseableIterator<T> createIteratorForReader(
			final RedissonClient client,
			final ReaderParams<T> readerParams,
			final String namespace,
			final boolean async ) {
		final Collection<SinglePartitionQueryRanges> ranges = readerParams.getQueryRanges().getPartitionQueryRanges();

		final Set<String> authorizations = Sets
				.newHashSet(
						readerParams.getAdditionalAuthorizations());
		if ((ranges != null) && !ranges.isEmpty()) {
			return createIterator(
					client,
					readerParams,
					namespace,
					ranges,
					authorizations,
					async);
		}
		else {
			final Iterator<GeoWaveRedisRow>[] iterators = new Iterator[readerParams.getAdapterIds().length];
			int i = 0;
			for (final short adapterId : readerParams.getAdapterIds()) {
				final Pair<Boolean, Boolean> groupByRowAndSortByTime = RedisUtils
						.isGroupByRowAndIsSortByTime(
								readerParams,
								adapterId);
				final String setNamePrefix = RedisUtils
						.getRowSetPrefix(
								namespace,
								readerParams
										.getInternalAdapterStore()
										.getTypeName(
												adapterId),
								readerParams.getIndex().getName());
				final Stream<Pair<ByteArray, Iterator<ScoredEntry<GeoWaveRedisPersistedRow>>>> streamIt = RedisUtils
						.getPartitions(
								client,
								setNamePrefix)
						.stream()
						.map(
								p -> {
									final Collection<ScoredEntry<GeoWaveRedisPersistedRow>> result = RedisUtils
											.getRowSet(
													client,
													setNamePrefix,
													p.getBytes(),
													groupByRowAndSortByTime.getRight())
											.entryRange(
													Double.NEGATIVE_INFINITY,
													true,
													Double.POSITIVE_INFINITY,
													true);
									final Iterator<ScoredEntry<GeoWaveRedisPersistedRow>> it = groupByRowAndSortByTime
											.getLeft()
													? RedisUtils
															.groupByRow(
																	result,
																	groupByRowAndSortByTime.getRight())
															.iterator()
													: result.iterator();
									return ImmutablePair
											.of(
													p,
													it);
								});
				iterators[i++] = streamIt
						.flatMap(
								p -> Streams
										.stream(
												Iterators
														.transform(
																p.getRight(),
																pr -> new GeoWaveRedisRow(
																		pr.getValue(),
																		adapterId,
																		p.getLeft().getBytes(),
																		RedisUtils
																				.getSortKey(
																						pr.getScore())))))
						.iterator();
			}
			return wrapResults(
					Iterators
							.concat(
									iterators),
					readerParams,
					authorizations);
		}
	}

	private CloseableIterator<T> createIterator(
			final RedissonClient client,
			final BaseReaderParams<T> readerParams,
			final String namespace,
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
								RedisUtils
										.getRowSetPrefix(
												namespace,
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
								RedisUtils
										.isGroupByRowAndIsSortByTime(
												readerParams,
												adapterId),
										RedisUtils.isSortByKeyRequired(readerParams)).results())
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
			final RedissonClient client,
			final RecordReaderParams<T> recordReaderParams,
			final String namespace ) {
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
				namespace,
				Collections.singleton(partitionRange),
				authorizations,
				// there should already be sufficient parallelism created by
				// input splits for record reader use cases
				false);
	}

	@SuppressWarnings("unchecked")
	private CloseableIterator<T> wrapResults(
			final Iterator<GeoWaveRedisRow> results,
			final BaseReaderParams<T> params,
			final Set<String> authorizations ) {
		return new CloseableIterator.Wrapper<>(
				params
						.getRowTransformer()
						.apply(
								sortBySortKeyIfRequired(
										params,
										(Iterator<GeoWaveRow>) (Iterator<? extends GeoWaveRow>) new GeoWaveRowMergingIterator<>(
												Iterators.filter(
														results,
														new ClientVisibilityFilter(
																authorizations))))));
	}

	private static Iterator<GeoWaveRow> sortBySortKeyIfRequired(
			final BaseReaderParams<?> params,
			final Iterator<GeoWaveRow> it ) {
		if (RedisUtils.isSortByKeyRequired(params)) {
			return RedisUtils.sortBySortKey(it);
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
