/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.mapreduce.splits;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.mapreduce.MapReduceUtils;

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
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore,
			final IndexStore indexStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {

		final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache = new HashMap<PrimaryIndex, RowRangeHistogramStatistics<?>>();

		final List<InputSplit> retVal = new ArrayList<InputSplit>();
		final TreeSet<IntermediateSplitInfo> splits = new TreeSet<IntermediateSplitInfo>();
		final Map<ByteArrayId, List<Short>> indexIdToAdaptersMap = new HashMap<>();
		for (final Pair<PrimaryIndex, List<Short>> indexAdapterIdPair : BaseDataStoreUtils
				.getAdaptersWithMinimalSetOfIndices(
						queryOptions,
						adapterStore,
						internalAdapterStore,
						adapterIndexMappingStore,
						indexStore)) {
			indexIdToAdaptersMap.put(
					indexAdapterIdPair.getKey().getId(),
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
					query,
					queryOptions.getAuthorizations());
		}

		// this is an incremental algorithm, it may be better use the target
		// split count to drive it (ie. to get 3 splits this will split 1
		// large
		// range into two down the middle and then split one of those ranges
		// down the middle to get 3, rather than splitting one range into
		// thirds)
		List<IntermediateSplitInfo> unsplittable = new ArrayList<IntermediateSplitInfo>();
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
			while (splits.size() != 0 && splits.size() + unsplittable.size() < minSplits);

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
					queryOptions.getAuthorizations()));
		}
		return retVal;
	}

	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final DataStoreOperations operations,
			final PrimaryIndex index,
			final List<Short> adapterIds,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final DistributableQuery query,
			final String[] authorizations )
			throws IOException {
		if ((query != null) && !query.isSupported(index)) {
			return splits;
		}

		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final int partitionKeyLength = indexStrategy.getPartitionKeyLength();

		// Build list of row ranges from query
		List<ByteArrayRange> ranges = null;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(index);
			if ((maxSplits != null) && (maxSplits > 0)) {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						maxSplits).getCompositeQueryRanges();
			}
			else {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						-1).getCompositeQueryRanges();
			}
		}

		RowRangeHistogramStatistics<?> statistics = getHistStats(
				index,
				adapterIds,
				adapterStore,
				statsStore,
				statsCache,
				authorizations);

		final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();
		if (ranges == null) {
			// Try to get ranges from histogram statistics
			TreeSet<ByteArrayId> partitionKeys = statistics.getPartitionKeys();
			for (ByteArrayId partitionKey : partitionKeys) {
				final GeoWaveRowRange gwRange = new GeoWaveRowRange(
						partitionKey.getBytes(),
						null,
						null,
						true,
						true);
				final double cardinality = getCardinality(
						statistics,
						gwRange,
						index.getIndexStrategy().getPartitionKeyLength());
				rangeList.add(new RangeLocationPair(
						gwRange,
						cardinality < 1 ? 1.0 : cardinality));
			}
		}
		else {
			for (final ByteArrayRange range : ranges) {
				final GeoWaveRowRange gwRange = SplitsProvider.toRowRange(
						range,
						partitionKeyLength);

				final double cardinality = getCardinality(
						statistics,
						gwRange,
						index.getIndexStrategy().getPartitionKeyLength());

				rangeList.add(new RangeLocationPair(
						gwRange,
						cardinality < 1 ? 1.0 : cardinality));
			}
		}

		final Map<ByteArrayId, SplitInfo> splitInfo = new HashMap<ByteArrayId, SplitInfo>();

		if (!rangeList.isEmpty()) {
			splitInfo.put(
					index.getId(),
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
			final GeoWaveRowRange range,
			final int partitionKeyLength ) {
		if (range == null) {
			if (rangeStats != null) {
				return rangeStats.totalSampleSize();
			}
			else {
				// with an infinite range and no histogram we have no info to
				// base a cardinality on
				return 1;
			}
		}
		return rangeStats == null ? getRangeLength(range) : rangeStats.cardinality(
				range.getPartitionKey(),
				range.getStartSortKey() == null ? null : ArrayUtils.subarray(
						range.getStartSortKey(),
						partitionKeyLength,
						range.getStartSortKey().length),
				range.getEndSortKey() == null ? null : ArrayUtils.subarray(
						range.getEndSortKey(),
						partitionKeyLength,
						range.getEndSortKey().length));
	}

	protected RowRangeHistogramStatistics<?> getHistStats(
			final PrimaryIndex index,
			final List<Short> adapterIds,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final String[] authorizations )
			throws IOException {
		RowRangeHistogramStatistics<?> rangeStats = statsCache.get(index);

		if (rangeStats == null) {
			try {
				rangeStats = getRangeStats(
						index,
						adapterIds,
						adapterStore,
						statsStore,
						authorizations);
			}
			catch (final Exception e) {
				throw new IOException(
						e);
			}
		}
		if (rangeStats != null) {
			statsCache.put(
					index,
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
			final PrimaryIndex index,
			final List<Short> adapterIds,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore store,
			final String[] authorizations ) {
		RowRangeHistogramStatistics<?> singleStats = null;
		for (final Short adapterId : adapterIds) {
			final RowRangeHistogramStatistics<?> rowStat = (RowRangeHistogramStatistics<?>) store.getDataStatistics(
					adapterId,
					RowRangeHistogramStatistics.composeId(index.getId()),
					authorizations);
			if (singleStats == null) {
				singleStats = rowStat;
			}
			else {
				singleStats.merge(rowStat);
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
					new ByteArrayId(
							startKey),
					new ByteArrayId(
							endKey));
		}
		else {
			final byte[] startKey = (range.getStartSortKey() == null) ? range.getPartitionKey() : ArrayUtils.addAll(
					range.getPartitionKey(),
					range.getStartSortKey());

			final byte[] endKey = (range.getEndSortKey() == null) ? ByteArrayId.getNextPrefix(range.getPartitionKey())
					: ArrayUtils.addAll(
							range.getPartitionKey(),
							range.getEndSortKey());

			return new ByteArrayRange(
					new ByteArrayId(
							startKey),
					new ByteArrayId(
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
