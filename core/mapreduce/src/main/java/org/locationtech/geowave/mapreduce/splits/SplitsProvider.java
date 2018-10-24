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
package org.locationtech.geowave.mapreduce.splits;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitsProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SplitsProvider.class);

	private static final BigInteger TWO = BigInteger.valueOf(2);

	public SplitsProvider() {}

	/**
	 * Read the metadata table to get tablets and match up ranges to them.
	 */
	public List<InputSplit> getSplits(
			final DataStoreOperations operations,
			final CommonQueryOptions commonOptions,
			final DataTypeQueryOptions<?> typeOptions,
			final IndexQueryOptions indexOptions,
			final QueryConstraints constraints,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore,
			final IndexStore indexStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {

		final Map<Pair<Index, ByteArray>, RowRangeHistogramStatistics<?>> statsCache = new HashMap<>();

		final List<InputSplit> retVal = new ArrayList<>();
		final TreeSet<IntermediateSplitInfo> splits = new TreeSet<>();
		final Map<String, List<Short>> indexIdToAdaptersMap = new HashMap<>();
		for (final Pair<Index, List<Short>> indexAdapterIdPair : BaseDataStoreUtils.getAdaptersWithMinimalSetOfIndices(
				typeOptions.getTypeNames(),
				indexOptions.getIndexName(),
				adapterStore,
				internalAdapterStore,
				adapterIndexMappingStore,
				indexStore)) {
			indexIdToAdaptersMap.put(
					indexAdapterIdPair.getKey().getName(),
					indexAdapterIdPair.getValue());
			populateIntermediateSplits(
					splits,
					operations,
					indexAdapterIdPair.getLeft(),
					indexAdapterIdPair.getValue(),
					statsCache,
					adapterStore,
					statsStore,
					maxSplits,
					constraints,
					(double[]) commonOptions.getHints().get(
							DataStoreUtils.TARGET_RESOLUTION_PER_DIMENSION_FOR_HIERARCHICAL_INDEX),
					commonOptions.getAuthorizations());
		}

		// this is an incremental algorithm, it may be better use the target
		// split count to drive it (ie. to get 3 splits this will split 1
		// large
		// range into two down the middle and then split one of those ranges
		// down the middle to get 3, rather than splitting one range into
		// thirds)
		final List<IntermediateSplitInfo> unsplittable = new ArrayList<>();
		if (!statsCache.isEmpty() && !splits.isEmpty() && (minSplits != null) && (splits.size() < minSplits)) {
			// set the ranges to at least min splits
			do {
				// remove the highest range, split it into 2 and add both
				// back,
				// increasing the size by 1
				final IntermediateSplitInfo highestSplit = splits.pollLast();
				final IntermediateSplitInfo otherSplit = highestSplit.split(statsCache);
				// When we can't split the highest split we remove it and
				// attempt the second highest
				// working our way up the split set.
				if (otherSplit == null) {
					unsplittable.add(highestSplit);
				}
				else {
					splits.add(highestSplit);
					splits.add(otherSplit);
				}
			}
			while ((splits.size() != 0) && ((splits.size() + unsplittable.size()) < minSplits));

			// Add all unsplittable splits back to splits array
			splits.addAll(unsplittable);

			if (splits.size() < minSplits) {
				LOGGER.warn("Truly unable to meet split count. Actual Count: " + splits.size());
			}
		}
		else if (((maxSplits != null) && (maxSplits > 0)) && (splits.size() > maxSplits)) {
			// merge splits to fit within max splits
			do {
				// this is the naive approach, remove the lowest two ranges
				// and merge them, decreasing the size by 1

				// TODO Ideally merge takes into account locations (as well
				// as possibly the index as a secondary criteria) to limit
				// the number of locations/indices
				final IntermediateSplitInfo lowestSplit = splits.pollFirst();
				final IntermediateSplitInfo nextLowestSplit = splits.pollFirst();
				lowestSplit.merge(nextLowestSplit);
				splits.add(lowestSplit);
			}
			while (splits.size() > maxSplits);
		}

		for (final IntermediateSplitInfo split : splits) {
			retVal.add(split.toFinalSplit(
					statsStore,
					indexIdToAdaptersMap,
					commonOptions.getAuthorizations()));
		}
		return retVal;
	}

	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final DataStoreOperations operations,
			final Index index,
			final List<Short> adapterIds,
			final Map<Pair<Index, ByteArray>, RowRangeHistogramStatistics<?>> statsCache,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final QueryConstraints constraints,
			final double[] targetResolutionPerDimensionForHierarchicalIndex,
			final String[] authorizations )
			throws IOException {

		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final int partitionKeyLength = indexStrategy.getPartitionKeyLength();

		// Build list of row ranges from query
		List<ByteArrayRange> ranges = null;
		if (constraints != null) {
			final List<MultiDimensionalNumericData> indexConstraints = constraints.getIndexConstraints(index);
			if ((maxSplits != null) && (maxSplits > 0)) {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						targetResolutionPerDimensionForHierarchicalIndex,
						maxSplits).getCompositeQueryRanges();
			}
			else {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						targetResolutionPerDimensionForHierarchicalIndex,
						-1).getCompositeQueryRanges();
			}
		}
		final List<RangeLocationPair> rangeList = new ArrayList<>();
		if (ranges == null) {

			final PartitionStatistics<?> statistics = getPartitionStats(
					index,
					adapterIds,
					statsStore,
					authorizations);

			// Try to get ranges from histogram statistics
			if (statistics != null) {
				final Set<ByteArray> partitionKeys = statistics.getPartitionKeys();
				for (final ByteArray partitionKey : partitionKeys) {
					final GeoWaveRowRange gwRange = new GeoWaveRowRange(
							partitionKey.getBytes(),
							null,
							null,
							true,
							true);
					final double cardinality = getCardinality(
							getHistStats(
									index,
									adapterIds,
									adapterStore,
									statsStore,
									statsCache,
									partitionKey,
									authorizations),
							gwRange);
					rangeList.add(new RangeLocationPair(
							gwRange,
							cardinality <= 0 ? 0 : cardinality < 1 ? 1.0 : cardinality));
				}
			}
			else {
				// add one all-inclusive range
				rangeList.add(new RangeLocationPair(
						new GeoWaveRowRange(
								null,
								null,
								null,
								true,
								false),
						0.0));
			}
		}
		else {
			for (final ByteArrayRange range : ranges) {
				final GeoWaveRowRange gwRange = SplitsProvider.toRowRange(
						range,
						partitionKeyLength);

				final double cardinality = getCardinality(
						getHistStats(
								index,
								adapterIds,
								adapterStore,
								statsStore,
								statsCache,
								new ByteArray(
										gwRange.getPartitionKey()),
								authorizations),
						gwRange);

				rangeList.add(new RangeLocationPair(
						gwRange,
						cardinality <= 0 ? 0 : cardinality < 1 ? 1.0 : cardinality));

			}
		}

		final Map<String, SplitInfo> splitInfo = new HashMap<>();

		if (!rangeList.isEmpty()) {
			splitInfo.put(
					index.getName(),
					new SplitInfo(
							index,
							rangeList));
			splits.add(new IntermediateSplitInfo(
					splitInfo,
					this));
		}

		return splits;
	}

	protected double getCardinality(
			final RowRangeHistogramStatistics<?> rangeStats,
			final GeoWaveRowRange range ) {
		if (range == null) {
			if (rangeStats != null) {
				return rangeStats.getTotalCount();
			}
			else {
				// with an infinite range and no histogram we have no info to
				// base a cardinality on
				return 0;
			}
		}

		return rangeStats == null ? 0.0 : rangeStats.cardinality(
				range.getStartSortKey(),
				range.getEndSortKey());
	}

	protected RowRangeHistogramStatistics<?> getHistStats(
			final Index index,
			final List<Short> adapterIds,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Map<Pair<Index, ByteArray>, RowRangeHistogramStatistics<?>> statsCache,
			final ByteArray partitionKey,
			final String[] authorizations )
			throws IOException {
		RowRangeHistogramStatistics<?> rangeStats = statsCache.get(Pair.of(
				index,
				partitionKey));

		if (rangeStats == null) {
			try {
				rangeStats = getRangeStats(
						index,
						adapterIds,
						adapterStore,
						statsStore,
						partitionKey,
						authorizations);
			}
			catch (final Exception e) {
				throw new IOException(
						e);
			}
		}
		if (rangeStats != null) {
			statsCache.put(
					Pair.of(
							index,
							partitionKey),
					rangeStats);
		}
		return rangeStats;
	}

	protected static byte[] getKeyFromBigInteger(
			final BigInteger value,
			final int numBytes ) {
		// TODO: does this account for the two extra bytes on BigInteger?
		final byte[] valueBytes = value.toByteArray();
		final byte[] bytes = new byte[numBytes];
		final int pos = Math.abs(numBytes - valueBytes.length);
		System.arraycopy(
				valueBytes,
				0,
				bytes,
				pos,
				Math.min(
						valueBytes.length,
						bytes.length));
		return bytes;
	}

	private RowRangeHistogramStatistics<?> getRangeStats(
			final Index index,
			final List<Short> adapterIds,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore store,
			final ByteArray partitionKey,
			final String[] authorizations ) {
		RowRangeHistogramStatistics<?> singleStats = null;

		final StatisticsQuery<NumericHistogram> statsQuery = StatisticsQueryBuilder
				.newBuilder()
				.factory()
				.rowHistogram()
				.indexName(
						index.getName())
				.partition(
						partitionKey)
				.build();
		for (final Short adapterId : adapterIds) {
			try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> it = store.getDataStatistics(
					adapterId,
					statsQuery.getExtendedId(),
					statsQuery.getStatsType(),
					authorizations)) {
				while (it.hasNext()) {
					final RowRangeHistogramStatistics<?> rowStat = (RowRangeHistogramStatistics<?>) it.next();
					if (singleStats == null) {
						singleStats = rowStat;
					}
					else {
						singleStats.merge(rowStat);
					}
				}
			}
		}

		return singleStats;
	}

	protected PartitionStatistics<?> getPartitionStats(
			final Index index,
			final List<Short> adapterIds,
			final DataStatisticsStore store,
			final String[] authorizations ) {
		PartitionStatistics<?> singleStats = null;
		final StatisticsQuery<Set<ByteArray>> statsQuery = StatisticsQueryBuilder
				.newBuilder()
				.factory()
				.partitions()
				.indexName(
						index.getName())
				.build();
		for (final Short adapterId : adapterIds) {
			try (CloseableIterator<InternalDataStatistics<?, ?, ?>> it = store.getDataStatistics(
					adapterId,
					statsQuery.getExtendedId(),
					statsQuery.getStatsType(),
					authorizations)) {
				while (it.hasNext()) {
					final PartitionStatistics<?> rowStat = (PartitionStatistics<?>) it.next();
					if (singleStats == null) {
						singleStats = rowStat;
					}
					else {
						singleStats.merge(rowStat);
					}
				}
			}
		}

		return singleStats;
	}

	protected static BigInteger getRange(
			final GeoWaveRowRange range,
			final int cardinality ) {
		return getEnd(
				range,
				cardinality).subtract(
				getStart(
						range,
						cardinality));
	}

	protected static BigInteger getStart(
			final GeoWaveRowRange range,
			final int cardinality ) {
		final byte[] start = range.getStartSortKey();
		byte[] startBytes;
		if (!range.isInfiniteStartSortKey() && (start != null)) {
			startBytes = extractBytes(
					start,
					cardinality);
		}
		else {
			startBytes = extractBytes(
					new byte[] {},
					cardinality);
		}
		return new BigInteger(
				startBytes);
	}

	protected static BigInteger getEnd(
			final GeoWaveRowRange range,
			final int cardinality ) {
		final byte[] end = range.getEndSortKey();
		byte[] endBytes;
		if (!range.isInfiniteStopSortKey() && (end != null)) {
			endBytes = extractBytes(
					end,
					cardinality);
		}
		else {
			endBytes = extractBytes(
					new byte[] {},
					cardinality,
					true);
		}

		return new BigInteger(
				endBytes);
	}

	protected static double getRangeLength(
			final GeoWaveRowRange range ) {
		if ((range == null) || (range.getStartSortKey() == null) || (range.getEndSortKey() == null)) {
			return 1;
		}
		final byte[] start = range.getStartSortKey();
		final byte[] end = range.getEndSortKey();

		final int maxDepth = Math.max(
				end.length,
				start.length);
		final BigInteger startBI = new BigInteger(
				extractBytes(
						start,
						maxDepth));
		final BigInteger endBI = new BigInteger(
				extractBytes(
						end,
						maxDepth));
		return endBI.subtract(
				startBI).doubleValue();
	}

	protected static byte[] getMidpoint(
			final GeoWaveRowRange range ) {
		if ((range.getStartSortKey() == null) || (range.getEndSortKey() == null)) {
			return null;
		}

		final byte[] start = range.getStartSortKey();
		final byte[] end = range.getEndSortKey();
		if (Arrays.equals(
				start,
				end)) {
			return null;
		}
		final int maxDepth = Math.max(
				end.length,
				start.length);
		final BigInteger startBI = new BigInteger(
				extractBytes(
						start,
						maxDepth));
		final BigInteger endBI = new BigInteger(
				extractBytes(
						end,
						maxDepth));
		final BigInteger rangeBI = endBI.subtract(startBI);
		if (rangeBI.equals(BigInteger.ZERO) || rangeBI.equals(BigInteger.ONE)) {
			return end;
		}
		final byte[] valueBytes = rangeBI.divide(
				TWO).add(
				startBI).toByteArray();
		final byte[] bytes = new byte[valueBytes.length - 2];
		System.arraycopy(
				valueBytes,
				2,
				bytes,
				0,
				bytes.length);
		return bytes;
	}

	public static byte[] extractBytes(
			final byte[] seq,
			final int numBytes ) {
		return extractBytes(
				seq,
				numBytes,
				false);
	}

	protected static byte[] extractBytes(
			final byte[] seq,
			final int numBytes,
			final boolean infiniteEndKey ) {
		final byte[] bytes = new byte[numBytes + 2];
		bytes[0] = 1;
		bytes[1] = 0;
		for (int i = 0; i < numBytes; i++) {
			if (i >= seq.length) {
				if (infiniteEndKey) {
					// -1 is 0xff
					bytes[i + 2] = -1;
				}
				else {
					bytes[i + 2] = 0;
				}
			}
			else {
				bytes[i + 2] = seq[i];
			}
		}
		return bytes;
	}

	public static GeoWaveRowRange toRowRange(
			final ByteArrayRange range,
			final int partitionKeyLength ) {
		final byte[] startRow = range.getStart() == null ? null : range.getStart().getBytes();
		final byte[] stopRow = range.getEnd() == null ? null : range.getEnd().getBytes();

		if (partitionKeyLength <= 0) {
			return new GeoWaveRowRange(
					null,
					startRow,
					stopRow,
					true,
					false);
		}
		else {
			byte[] partitionKey;
			boolean partitionKeyDiffers = false;
			if ((startRow == null) && (stopRow == null)) {
				return new GeoWaveRowRange(
						null,
						null,
						null,
						true,
						true);
			}
			else if (startRow != null) {
				partitionKey = ArrayUtils.subarray(
						startRow,
						0,
						partitionKeyLength);
				if (stopRow != null) {
					partitionKeyDiffers = !Arrays.equals(
							partitionKey,
							ArrayUtils.subarray(
									stopRow,
									0,
									partitionKeyLength));
				}
			}
			else {
				partitionKey = ArrayUtils.subarray(
						stopRow,
						0,
						partitionKeyLength);
			}
			return new GeoWaveRowRange(
					partitionKey,
					startRow == null ? null : (partitionKeyLength == startRow.length ? null : ArrayUtils.subarray(
							startRow,
							partitionKeyLength,
							startRow.length)),
					partitionKeyDiffers ? null : (stopRow == null ? null : (partitionKeyLength == stopRow.length ? null
							: ArrayUtils.subarray(
									stopRow,
									partitionKeyLength,
									stopRow.length))),
					true,
					partitionKeyDiffers);

		}
	}

	public static ByteArrayRange fromRowRange(
			final GeoWaveRowRange range ) {

		if ((range.getPartitionKey() == null) || (range.getPartitionKey().length == 0)) {
			final byte[] startKey = (range.getStartSortKey() == null) ? null : range.getStartSortKey();
			final byte[] endKey = (range.getEndSortKey() == null) ? null : range.getEndSortKey();

			return new ByteArrayRange(
					new ByteArray(
							startKey),
					new ByteArray(
							endKey));
		}
		else {
			final byte[] startKey = (range.getStartSortKey() == null) ? range.getPartitionKey() : ArrayUtils.addAll(
					range.getPartitionKey(),
					range.getStartSortKey());

			final byte[] endKey = (range.getEndSortKey() == null) ? ByteArray.getNextPrefix(range.getPartitionKey())
					: ArrayUtils.addAll(
							range.getPartitionKey(),
							range.getEndSortKey());

			return new ByteArrayRange(
					new ByteArray(
							startKey),
					new ByteArray(
							endKey));
		}
	}

	public static byte[] getInclusiveEndKey(
			final byte[] endKey ) {
		final byte[] inclusiveEndKey = new byte[endKey.length + 1];

		System.arraycopy(
				endKey,
				0,
				inclusiveEndKey,
				0,
				inclusiveEndKey.length - 1);

		return inclusiveEndKey;
	}
}
