/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.datastore.redis.operations;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.util.RowConsumer;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisPersistedRow;
import org.locationtech.geowave.datastore.redis.util.GeoWaveRedisRow;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RFuture;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;

public class BatchedRangeRead<T>
{
	private final static Logger LOGGER = LoggerFactory
			.getLogger(
					BatchedRangeRead.class);

	private static class RangeReadInfo
	{
		byte[] partitionKey;
		double startScore;
		double endScore;

		public RangeReadInfo(
				final byte[] partitionKey,
				final double startScore,
				final double endScore ) {
			this.partitionKey = partitionKey;
			this.startScore = startScore;
			this.endScore = endScore;
		}
	}

	private static class ScoreOrderComparator implements
			Comparator<RangeReadInfo>,
			Serializable
	{
		private static final long serialVersionUID = 1L;
		private static final ScoreOrderComparator SINGLETON = new ScoreOrderComparator();

		@Override
		public int compare(
				final RangeReadInfo o1,
				final RangeReadInfo o2 ) {
			int comp = Double
					.compare(
							o1.startScore,
							o2.startScore);
			if (comp != 0) {
				return comp;
			}
			comp = Double
					.compare(
							o1.endScore,
							o2.endScore);
			if (comp != 0) {
				return comp;
			}
			final byte[] otherComp = o2.partitionKey == null ? new byte[0] : o2.partitionKey;
			final byte[] thisComp = o1.partitionKey == null ? new byte[0] : o1.partitionKey;

			return UnsignedBytes
					.lexicographicalComparator()
					.compare(
							thisComp,
							otherComp);
		}

	}

	private final static int MAX_CONCURRENT_READ = 100;
	private final static int MAX_BOUNDED_READS_ENQUEUED = 1000000;
	private static ByteArray EMPTY_PARTITION_KEY = new ByteArray();
	private final LoadingCache<ByteArray, RScoredSortedSet<GeoWaveRedisPersistedRow>> setCache = Caffeine
			.newBuilder()
			.build(
					partitionKey -> getSet(
							partitionKey.getBytes()));
	private final Collection<SinglePartitionQueryRanges> ranges;
	private final short adapterId;
	private final String setNamePrefix;
	private final RedissonClient client;
	private final GeoWaveRowIteratorTransformer<T> rowTransformer;
	private final Predicate<GeoWaveRow> filter;

	// only allow so many outstanding async reads or writes, use this semaphore
	// to control it
	private final Semaphore readSemaphore = new Semaphore(
			MAX_CONCURRENT_READ);
	private final boolean async;
	private final Pair<Boolean, Boolean> groupByRowAndSortByTimePair;
	private final boolean isSortFinalResultsBySortKey;

	protected BatchedRangeRead(
			final RedissonClient client,
			final String setNamePrefix,
			final short adapterId,
			final Collection<SinglePartitionQueryRanges> ranges,
			final GeoWaveRowIteratorTransformer<T> rowTransformer,
			final Predicate<GeoWaveRow> filter,
			final boolean async,
			final Pair<Boolean, Boolean> groupByRowAndSortByTimePair,
			final boolean isSortFinalResultsBySortKey ) {
		this.client = client;
		this.setNamePrefix = setNamePrefix;
		this.adapterId = adapterId;
		this.ranges = ranges;
		this.rowTransformer = rowTransformer;
		this.filter = filter;
		// we can't efficiently guarantee sort order with async queries
		this.async = async && !isSortFinalResultsBySortKey;
		this.groupByRowAndSortByTimePair = groupByRowAndSortByTimePair;
		this.isSortFinalResultsBySortKey = isSortFinalResultsBySortKey;
	}

	private RScoredSortedSet<GeoWaveRedisPersistedRow> getSet(
			final byte[] partitionKey ) {
		return RedisUtils
				.getRowSet(
						client,
						setNamePrefix,
						partitionKey,
						groupByRowAndSortByTimePair.getRight());

	}

	public CloseableIterator<T> results() {
		final List<RangeReadInfo> reads = new ArrayList<>();
		for (final SinglePartitionQueryRanges r : ranges) {
			for (final ByteArrayRange range : r.getSortKeyRanges()) {
				final double start = range.getStart() != null ? RedisUtils
						.getScore(
								range.getStart().getBytes())
						: Double.NEGATIVE_INFINITY;
				final double end = range.getEnd() != null ? RedisUtils
						.getScore(
								range.getEndAsNextPrefix().getBytes())
						: Double.POSITIVE_INFINITY;
				reads
						.add(
								new RangeReadInfo(
										r.getPartitionKey().getBytes(),
										start,
										end));
			}

		}
		if (async) {
			return executeQueryAsync(
					reads);
		}
		else {
			return executeQuery(
					reads);
		}
	}

	public CloseableIterator<T> executeQuery(
			final List<RangeReadInfo> reads ) {
		if (isSortFinalResultsBySortKey) {
			// order the reads by sort keys
			reads
					.sort(
							ScoreOrderComparator.SINGLETON);
		}
		return new CloseableIterator.Wrapper<>(
				Iterators
						.concat(
								reads
										.stream()
										.map(
												r -> {
													ByteArray partitionKey;
													if ((r.partitionKey == null) || (r.partitionKey.length == 0)) {
														partitionKey = EMPTY_PARTITION_KEY;
													}
													else {
														partitionKey = new ByteArray(
																r.partitionKey);
													}
													// if we don't have enough
													// precision we need to make
													// sure the end is inclusive
													return transformAndFilter(
															setCache
																	.get(
																			partitionKey)
																	.entryRange(
																			r.startScore,
																			true,
																			r.endScore,
																			r.endScore <= r.startScore),
															r.partitionKey);
												})
										.iterator()));
	}

	public CloseableIterator<T> executeQueryAsync(
			final List<RangeReadInfo> reads ) {
		// first create a list of asynchronous query executions
		final List<RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>>> futures = Lists
				.newArrayListWithExpectedSize(
						reads.size());
		final BlockingQueue<Object> results = new LinkedBlockingQueue<>(
				MAX_BOUNDED_READS_ENQUEUED);
		new Thread(
				new Runnable() {
					@Override
					public void run() {
						// set it to 1 to make sure all queries are submitted in
						// the loop
						final AtomicInteger queryCount = new AtomicInteger(
								1);
						for (final RangeReadInfo r : reads) {
							try {
								ByteArray partitionKey;
								if ((r.partitionKey == null) || (r.partitionKey.length == 0)) {
									partitionKey = EMPTY_PARTITION_KEY;
								}
								else {
									partitionKey = new ByteArray(
											r.partitionKey);
								}
								readSemaphore.acquire();
								final RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>> f = setCache
										.get(
												partitionKey)
										.entryRangeAsync(
												r.startScore,
												true,
												r.endScore,
												// if we don't have enough
												// precision we need to make
												// sure the end is inclusive
												r.endScore <= r.startScore);
								queryCount.incrementAndGet();
								f
										.handle(
												(
														result,
														throwable ) -> {
													if (!f.isSuccess()) {
														if (!f.isCancelled()) {
															LOGGER
																	.warn(
																			"Async Redis query failed",
																			throwable);
														}
														checkFinalize(
																readSemaphore,
																results,
																queryCount);
														return result;
													}
													else {
														try {
															transformAndFilter(
																	result,
																	r.partitionKey)
																			.forEachRemaining(
																					row -> {
																						try {
																							results
																									.put(
																											row);
																						}
																						catch (final InterruptedException e) {
																							LOGGER
																									.warn(
																											"interrupted while waiting to enqueue a redis result",
																											e);
																						}
																					});

														}
														finally {
															checkFinalize(
																	readSemaphore,
																	results,
																	queryCount);
														}
														return result;
													}
												});
								synchronized (futures) {

									futures
											.add(
													f);

								}
							}
							catch (final InterruptedException e) {
								LOGGER
										.warn(
												"Exception while executing query",
												e);
								readSemaphore.release();
							}
						}
						// then decrement
						if (queryCount.decrementAndGet() <= 0) {
							// and if there are no queries, there may not have
							// been any
							// statements submitted
							try {
								results
										.put(
												RowConsumer.POISON);
							}
							catch (final InterruptedException e) {
								LOGGER
										.error(
												"Interrupted while finishing blocking queue, this may result in deadlock!");
							}
						}
					}
				},
				"Redis Query Executor").start();
		return new CloseableIteratorWrapper<>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						List<RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>>> newFutures;
						synchronized (futures) {
							newFutures = new ArrayList<>(
									futures);
						}
						for (final RFuture<Collection<ScoredEntry<GeoWaveRedisPersistedRow>>> f : newFutures) {
							f
									.cancel(
											true);
						}
					}
				},
				new RowConsumer<>(
						results));
	}

	private Iterator<T> transformAndFilter(
			final Collection<ScoredEntry<GeoWaveRedisPersistedRow>> result,
			final byte[] partitionKey ) {
		return rowTransformer
				.apply(
						sortByKeyIfRequired(
								isSortFinalResultsBySortKey,
								(Iterator<GeoWaveRow>) (Iterator<? extends GeoWaveRow>) new GeoWaveRowMergingIterator<>(
										Iterators
												.filter(
														Iterators
																.transform(
																		groupByRowAndSortByTimePair.getLeft()
																				? RedisUtils
																						.groupByRow(
																								result,
																								groupByRowAndSortByTimePair
																										.getRight())
																						.iterator()
																				: result.iterator(),
																		new Function<ScoredEntry<GeoWaveRedisPersistedRow>, GeoWaveRedisRow>() {

																			@Override
																			public GeoWaveRedisRow apply(
																					final ScoredEntry<GeoWaveRedisPersistedRow> entry ) {
																		// @formatter:off
																		// wrap the persisted row with additional metadata
																		// @formatter:on
																				return new GeoWaveRedisRow(
																						entry.getValue(),
																						adapterId,
																						partitionKey,
																						RedisUtils
																								.getSortKey(
																										entry
																												.getScore()));
																			}
																		}),
														filter))));
	}

	private static Iterator<GeoWaveRow> sortByKeyIfRequired(
			final boolean isRequired,
			final Iterator<GeoWaveRow> it ) {
		if (isRequired) {
			return RedisUtils
					.sortBySortKey(
							it);
		}
		return it;
	}

	private static void checkFinalize(
			final Semaphore semaphore,
			final BlockingQueue<Object> resultQueue,
			final AtomicInteger queryCount ) {
		semaphore.release();
		if (queryCount.decrementAndGet() <= 0) {
			try {
				resultQueue
						.put(
								RowConsumer.POISON);
			}
			catch (final InterruptedException e) {
				LOGGER
						.error(
								"Interrupted while finishing blocking queue, this may result in deadlock!");
			}
		}
	}
}
