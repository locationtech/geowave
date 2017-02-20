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
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public abstract class SplitsProvider
{
	private final static Logger LOGGER = Logger.getLogger(SplitsProvider.class);

	private static final BigInteger TWO = BigInteger.valueOf(2);

	public SplitsProvider() {}

	/**
	 * Read the metadata table to get tablets and match up ranges to them.
	 */
	public List<InputSplit> getSplits(
			final DataStoreOperations operations,
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {

		final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache = new HashMap<PrimaryIndex, RowRangeHistogramStatistics<?>>();

		final List<InputSplit> retVal = new ArrayList<InputSplit>();
		final TreeSet<IntermediateSplitInfo> splits = new TreeSet<IntermediateSplitInfo>();
		final Map<ByteArrayId, List<ByteArrayId>> indexIdToAdaptersMap = new HashMap<>();
		for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : queryOptions
				.getAdaptersWithMinimalSetOfIndices(
						adapterStore,
						adapterIndexMappingStore,
						indexStore)) {
			indexIdToAdaptersMap.put(
					indexAdapterPair.getKey().getId(),
					Lists.transform(
							indexAdapterPair.getValue(),
							new Function<DataAdapter<Object>, ByteArrayId>() {

								@Override
								public ByteArrayId apply(
										final DataAdapter<Object> input ) {
									return input.getAdapterId();
								}
							}));
			populateIntermediateSplits(
					splits,
					operations,
					indexAdapterPair.getLeft(),
					indexAdapterPair.getValue(),
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
		if (!statsCache.isEmpty() && !splits.isEmpty() && (minSplits != null) && (splits.size() < minSplits)) {
			// set the ranges to at least min splits
			do {
				// remove the highest range, split it into 2 and add both
				// back,
				// increasing the size by 1
				final IntermediateSplitInfo highestSplit = splits.pollLast();
				final IntermediateSplitInfo otherSplit = highestSplit.split(statsCache);
				splits.add(highestSplit);
				if (otherSplit == null) {
					LOGGER.warn("Cannot meet minimum splits");
					break;
				}
				splits.add(otherSplit);
			}
			while (splits.size() < minSplits);
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

	protected abstract TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			TreeSet<IntermediateSplitInfo> splits,
			DataStoreOperations operations,
			PrimaryIndex left,
			List<DataAdapter<Object>> value,
			Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			AdapterStore adapterStore,
			DataStatisticsStore statsStore,
			Integer maxSplits,
			DistributableQuery query,
			String[] authorizations )
			throws IOException;

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
			final List<DataAdapter<Object>> adapters,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final String[] authorizations )
			throws IOException {
		RowRangeHistogramStatistics<?> rangeStats = statsCache.get(index);

		if (rangeStats == null) {
			try {
				rangeStats = getRangeStats(
						index,
						adapters,
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
			final List<DataAdapter<Object>> adapters,
			final AdapterStore adapterStore,
			final DataStatisticsStore store,
			final String[] authorizations ) {
		RowRangeHistogramStatistics<?> singleStats = null;
		for (final DataAdapter<?> adapter : adapters) {
			final RowRangeHistogramStatistics<?> rowStat = (RowRangeHistogramStatistics<?>) store.getDataStatistics(
					adapter.getAdapterId(),
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

}
