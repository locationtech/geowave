package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.RangeLocationPair;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.input.RangeLocationPair.HBaseMRRowRange;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseMRUtils
{
	private final static Logger LOGGER = Logger.getLogger(HBaseMRUtils.class);

	/**
	 * Read the metadata table to get tablets and match up ranges to them.
	 */
	public static List<InputSplit> getSplits(
			final BasicHBaseOperations operations,
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

		for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : queryOptions
				.getAdaptersWithMinimalSetOfIndices(
						adapterStore,
						adapterIndexMappingStore,
						indexStore)) {

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
				// and
				// merge them, decreasing the size by 1

				// TODO Ideally merge takes into account locations (as well
				// as
				// possibly the index as a secondary criteria) to limit the
				// number of locations/indices
				final IntermediateSplitInfo lowestSplit = splits.pollFirst();
				final IntermediateSplitInfo nextLowestSplit = splits.pollFirst();
				lowestSplit.merge(nextLowestSplit);
				splits.add(lowestSplit);
			}
			while (splits.size() > maxSplits);
		}

		for (final IntermediateSplitInfo split : splits) {
			retVal.add(split.toFinalSplit());
		}
		return retVal;
	}

	private static final BigInteger ONE = new BigInteger(
			"1");

	private static RowRangeHistogramStatistics<?> getRangeStats(
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

	private static HBaseMRRowRange getRangeMax(
			final Index<?, ?> index,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final String[] authorizations ) {

		final RowRangeDataStatistics<?> stats = (RowRangeDataStatistics<?>) statsStore.getDataStatistics(
				index.getId(),
				RowRangeDataStatistics.getId(index.getId()),
				authorizations);
		if (stats == null) {
			LOGGER
					.warn("Could not determine range of data from 'RowRangeDataStatistics'.  Range will not be clipped. This may result in some splits being empty.");
			return new HBaseMRRowRange();
		}

		final int cardinality = Math.max(
				stats.getMin().length,
				stats.getMax().length);
		return new HBaseMRRowRange(
				new ByteArrayId(
						getKeyFromBigInteger(
								new BigInteger(
										stats.getMin()).subtract(ONE),
								cardinality)),
				new ByteArrayId(
						getKeyFromBigInteger(
								new BigInteger(
										stats.getMax()).add(ONE),
								cardinality)));
	}

	private static TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final BasicHBaseOperations operations,
			final PrimaryIndex index,
			final List<DataAdapter<Object>> adapters,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final DistributableQuery query,
			final String[] authorizations )
			throws IOException {

		if ((query != null) && !query.isSupported(index)) {
			return splits;
		}
		final HBaseMRRowRange fullrange = getRangeMax(
				index,
				adapterStore,
				statsStore,
				authorizations);

		final String tableName = index.getId().getString();
		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();

		// Build list of row ranges from query
		List<HBaseMRRowRange> ranges = new ArrayList<HBaseMRRowRange>();
		final List<ByteArrayRange> constraintRanges;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(indexStrategy);
			if ((maxSplits != null) && (maxSplits > 0)) {
				constraintRanges = DataStoreUtils.constraintsToByteArrayRanges(
						indexConstraints,
						indexStrategy,
						maxSplits);
			}
			else {
				constraintRanges = DataStoreUtils.constraintsToByteArrayRanges(
						indexConstraints,
						indexStrategy,
						-1);
			}
			for (final ByteArrayRange constraintRange : constraintRanges) {
				ranges.add(new HBaseMRRowRange(
						constraintRange));
			}
		}
		else {
			ranges.add(fullrange);
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Protected range: " + fullrange);
			}
		}

		final Map<HRegionLocation, Map<HRegionInfo, List<HBaseMRRowRange>>> binnedRanges = new HashMap<HRegionLocation, Map<HRegionInfo, List<HBaseMRRowRange>>>();
		final RegionLocator regionLocator = operations.getRegionLocator(tableName);
		while (!ranges.isEmpty()) {
			ranges = binRanges(
					ranges,
					binnedRanges,
					regionLocator);
		}

		for (final Entry<HRegionLocation, Map<HRegionInfo, List<HBaseMRRowRange>>> locationEntry : binnedRanges
				.entrySet()) {
			final String hostname = locationEntry.getKey().getHostname();

			for (final Entry<HRegionInfo, List<HBaseMRRowRange>> regionEntry : locationEntry.getValue().entrySet()) {
				final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo = new HashMap<PrimaryIndex, List<RangeLocationPair>>();
				final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();

				for (final HBaseMRRowRange range : regionEntry.getValue()) {

					final double cardinality = getCardinality(
							getHistStats(
									index,
									adapters,
									adapterStore,
									statsStore,
									statsCache,
									authorizations),
							range);

					if (range.intersects(fullrange)) {
						rangeList.add(new RangeLocationPair(
								range,
								hostname,
								cardinality < 1 ? 1.0 : cardinality));
					}
					else {
						LOGGER.info("Query split outside of range");
					}
					if (LOGGER.isTraceEnabled()) {
						LOGGER.warn("Clipped range: " + rangeList.get(
								rangeList.size() - 1).getRange());
					}
				}

				if (!rangeList.isEmpty()) {
					splitInfo.put(
							index,
							rangeList);
					splits.add(new IntermediateSplitInfo(
							splitInfo));
				}
			}
		}

		return splits;
	}

	private static List<HBaseMRRowRange> binRanges(
			final List<HBaseMRRowRange> inputRanges,
			final Map<HRegionLocation, Map<HRegionInfo, List<HBaseMRRowRange>>> binnedRanges,
			final RegionLocator regionLocator )
			throws IOException {

		// Loop through ranges, getting RegionLocation and RegionInfo for
		// startKey, clipping range by that regionInfo's extent, and leaving
		// remainder in the List to be region'd
		final ListIterator<HBaseMRRowRange> i = inputRanges.listIterator();
		while (i.hasNext()) {
			final HBaseMRRowRange range = i.next();
			final HRegionLocation location = regionLocator.getRegionLocation(range.getStart().getBytes());

			Map<HRegionInfo, List<HBaseMRRowRange>> regionInfoMap = binnedRanges.get(location);
			if (regionInfoMap == null) {
				regionInfoMap = new HashMap<HRegionInfo, List<HBaseMRRowRange>>();
				binnedRanges.put(
						location,
						regionInfoMap);
			}

			final HRegionInfo regionInfo = location.getRegionInfo();
			List<HBaseMRRowRange> rangeList = regionInfoMap.get(regionInfo);
			if (rangeList == null) {
				rangeList = new ArrayList<HBaseMRRowRange>();
				regionInfoMap.put(
						regionInfo,
						rangeList);
			}

			if (regionInfo.containsRange(
					range.getStart().getBytes(),
					range.getEnd().getBytes())) {
				rangeList.add(range);
				i.remove();
			}
			else {
				final ByteArrayRange overlappingRange = range.intersection(new ByteArrayRange(
						new ByteArrayId(
								regionInfo.getStartKey()),
						new ByteArrayId(
								regionInfo.getEndKey())));
				rangeList.add(new HBaseMRRowRange(
						overlappingRange));

				final HBaseMRRowRange uncoveredRange = new HBaseMRRowRange(
						new ByteArrayId(
								HBaseUtils.getNextPrefix(regionInfo.getEndKey())),
						range.getEnd());
				i.add(uncoveredRange);
			}
		}

		return inputRanges;
	}

	private static double getCardinality(
			final RowRangeHistogramStatistics<?> rangeStats,
			final HBaseMRRowRange range ) {
		return rangeStats == null ? getRangeLength(range) : rangeStats.cardinality(
				range.getStart().getBytes(),
				range.getEnd().getBytes());
	}

	private static RowRangeHistogramStatistics<?> getHistStats(
			final PrimaryIndex index,
			final List<DataAdapter<Object>> adapters,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final String[] authorizations )
			throws IOException {
		RowRangeHistogramStatistics<?> rangeStats = statsCache.get(index);

		if (rangeStats == null) {
			rangeStats = getRangeStats(
					index,
					adapters,
					adapterStore,
					statsStore,
					authorizations);
		}
		if (rangeStats != null) {
			statsCache.put(
					index,
					rangeStats);
		}
		return rangeStats;
	}

	protected static class IntermediateSplitInfo implements
			Comparable<IntermediateSplitInfo>
	{
		protected static class IndexRangeLocation
		{
			private RangeLocationPair rangeLocationPair;
			private final PrimaryIndex index;

			public IndexRangeLocation(
					final RangeLocationPair rangeLocationPair,
					final PrimaryIndex index ) {
				this.rangeLocationPair = rangeLocationPair;
				this.index = index;
			}

			public IndexRangeLocation split(
					final RowRangeHistogramStatistics<?> stats,
					final double currentCardinality,
					final double targetCardinality ) {

				if (stats == null) {
					return null;
				}

				final double thisCardinalty = rangeLocationPair.getCardinality();
				final double fraction = (targetCardinality - currentCardinality) / thisCardinalty;
				final int splitCardinality = (int) (thisCardinalty * fraction);

				final byte[] start = rangeLocationPair.getRange().getStart().getBytes();
				final byte[] end = rangeLocationPair.getRange().getEnd().getBytes();

				final double cdfStart = stats.cdf(start);
				final double cdfEnd = stats.cdf(end);
				final byte[] expectedEnd = stats.quantile(cdfStart + ((cdfEnd - cdfStart) * fraction));

				final int maxCardinality = Math.max(
						start.length,
						end.length);

				final byte[] splitKey = expandBytes(
						expectedEnd,
						maxCardinality);

				if (Arrays.equals(
						start,
						splitKey) || Arrays.equals(
						end,
						splitKey)) {
					return null;
				}

				final String location = rangeLocationPair.getLocation();
				try {
					final RangeLocationPair newPair = new RangeLocationPair(
							new HBaseMRRowRange(
									rangeLocationPair.getRange().getStart(),
									new ByteArrayId(
											splitKey)),
							location,
							splitCardinality);

					rangeLocationPair = new RangeLocationPair(
							new HBaseMRRowRange(
									new ByteArrayId(
											splitKey),
									rangeLocationPair.getRange().getEnd()),
							location,
							rangeLocationPair.getCardinality() - splitCardinality);

					return new IndexRangeLocation(
							newPair,
							index);
				}
				catch (final java.lang.IllegalArgumentException ex) {
					LOGGER.info("Unable to split range: " + ex.getLocalizedMessage());
					return null;
				}
			}
		}

		private final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo;

		public IntermediateSplitInfo(
				final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo ) {
			this.splitInfo = splitInfo;
		}

		private synchronized void merge(
				final IntermediateSplitInfo split ) {
			for (final Entry<PrimaryIndex, List<RangeLocationPair>> e : split.splitInfo.entrySet()) {
				List<RangeLocationPair> thisList = splitInfo.get(e.getKey());
				if (thisList == null) {
					thisList = new ArrayList<RangeLocationPair>();
					splitInfo.put(
							e.getKey(),
							thisList);
				}
				thisList.addAll(e.getValue());
			}
		}

		/**
		 * Side effect: Break up this split.
		 * 
		 * Split the ranges into two
		 * 
		 * @return the new split.
		 */
		private synchronized IntermediateSplitInfo split(
				final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache ) {
			// generically you'd want the split to be as limiting to total
			// locations as possible and then as limiting as possible to total
			// indices, but in this case split() is only called when all ranges
			// are in the same location and the same index

			final TreeSet<IndexRangeLocation> orderedSplits = new TreeSet<IndexRangeLocation>(
					new Comparator<IndexRangeLocation>() {

						@Override
						public int compare(
								final IndexRangeLocation o1,
								final IndexRangeLocation o2 ) {
							return (o1.rangeLocationPair.getCardinality() - o2.rangeLocationPair.getCardinality()) < 0 ? -1
									: 1;
						}
					});
			for (final Entry<PrimaryIndex, List<RangeLocationPair>> ranges : splitInfo.entrySet()) {
				for (final RangeLocationPair p : ranges.getValue()) {
					orderedSplits.add(new IndexRangeLocation(
							p,
							ranges.getKey()));
				}
			}
			final double targetCardinality = getTotalRangeAtCardinality() / 2;
			double currentCardinality = 0.0;
			final Map<PrimaryIndex, List<RangeLocationPair>> otherSplitInfo = new HashMap<PrimaryIndex, List<RangeLocationPair>>();

			splitInfo.clear();

			do {
				final IndexRangeLocation next = orderedSplits.pollFirst();
				final double nextCardinality = currentCardinality + next.rangeLocationPair.getCardinality();
				if (nextCardinality > targetCardinality) {
					final IndexRangeLocation newSplit = next.split(
							statsCache.get(next.index),
							currentCardinality,
							targetCardinality);
					// Stats can have inaccuracies over narrow ranges
					// thus, a split based on statistics may not be found
					if (newSplit != null) {
						addPairForIndex(
								otherSplitInfo,
								newSplit.rangeLocationPair,
								newSplit.index);
						addPairForIndex(
								splitInfo,
								next.rangeLocationPair,
								next.index);
					}
					else {
						// Still add to the other SPLIT if there is remaining
						// pairs
						// in this SPLIT
						addPairForIndex(
								(!orderedSplits.isEmpty()) ? otherSplitInfo : splitInfo,
								next.rangeLocationPair,
								next.index);
					}

					break;
				}
				else {
					addPairForIndex(
							otherSplitInfo,
							next.rangeLocationPair,
							next.index);
					currentCardinality = nextCardinality;
				}
			}
			while (!orderedSplits.isEmpty());

			// What is left of the ranges
			// that haven't been placed in the other split info

			for (final IndexRangeLocation split : orderedSplits) {
				addPairForIndex(
						splitInfo,
						split.rangeLocationPair,
						split.index);
			}
			// All ranges consumed by the other split
			if (splitInfo.size() == 0) {
				// First try to move a index set of ranges back.
				if (otherSplitInfo.size() > 1) {
					final Iterator<Entry<PrimaryIndex, List<RangeLocationPair>>> it = otherSplitInfo
							.entrySet()
							.iterator();
					final Entry<PrimaryIndex, List<RangeLocationPair>> entry = it.next();
					it.remove();
					splitInfo.put(
							entry.getKey(),
							entry.getValue());
				}
				else {
					splitInfo.putAll(otherSplitInfo);
					otherSplitInfo.clear();
				}
			}

			return otherSplitInfo.size() == 0 ? null : new IntermediateSplitInfo(
					otherSplitInfo);
		}

		private void addPairForIndex(
				final Map<PrimaryIndex, List<RangeLocationPair>> otherSplitInfo,
				final RangeLocationPair pair,
				final PrimaryIndex index ) {
			List<RangeLocationPair> list = otherSplitInfo.get(index);
			if (list == null) {
				list = new ArrayList<RangeLocationPair>();
				otherSplitInfo.put(
						index,
						list);
			}
			list.add(pair);

		}

		private synchronized GeoWaveHBaseInputSplit toFinalSplit() {
			final Set<String> locations = new HashSet<String>();
			for (final Entry<PrimaryIndex, List<RangeLocationPair>> entry : splitInfo.entrySet()) {
				for (final RangeLocationPair pair : entry.getValue()) {
					locations.add(pair.getLocation());
				}
			}
			return new GeoWaveHBaseInputSplit(
					splitInfo,
					locations.toArray(new String[locations.size()]));
		}

		@Override
		public int compareTo(
				final IntermediateSplitInfo o ) {
			final double thisTotal = getTotalRangeAtCardinality();
			final double otherTotal = o.getTotalRangeAtCardinality();
			return (thisTotal - otherTotal) < 0 ? -1 : 1;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof IntermediateSplitInfo)) {
				return false;
			}
			return compareTo((IntermediateSplitInfo) obj) == 0;
		}

		@Override
		public int hashCode() {
			// think this matches the spirit of compareTo
			return com.google.common.base.Objects.hashCode(
					getTotalRangeAtCardinality(),
					super.hashCode());
		}

		private synchronized double getTotalRangeAtCardinality() {
			double sum = 0.0;
			for (final List<RangeLocationPair> pairList : splitInfo.values()) {
				for (final RangeLocationPair pair : pairList) {
					sum += pair.getCardinality();
				}
			}
			return sum;
		}
	}

	protected static byte[] expandBytes(
			final byte valueBytes[],
			final int numBytes ) {
		final byte[] bytes = new byte[numBytes];
		for (int i = 0; i < numBytes; i++) {
			if (i < valueBytes.length) {
				bytes[i] = valueBytes[i];
			}
			else {
				bytes[i] = 0;
			}
		}
		return bytes;
	}

	protected static byte[] getKeyFromBigInteger(
			final BigInteger value,
			final int numBytes ) {
		final byte[] valueBytes = value.toByteArray();
		final byte[] bytes = new byte[numBytes];
		System.arraycopy(
				valueBytes,
				0,
				bytes,
				0,
				Math.min(
						valueBytes.length,
						bytes.length));
		return bytes;
	}

	protected static byte[] extractBytes(
			final ByteArrayId id,
			final int numBytes ) {
		final byte[] bytes = new byte[numBytes + 2];
		bytes[0] = 1;
		bytes[1] = 0;
		for (int i = 0; i < numBytes; i++) {
			if (i >= id.getBytes().length) {
				bytes[i + 2] = 0;
			}
			else {
				bytes[i + 2] = id.getBytes()[i];
			}
		}
		return bytes;
	}

	// protected static TabletLocator getTabletLocator(
	// final Object clientContextOrInstance,
	// final String tableId )
	// throws TableNotFoundException {
	// TabletLocator tabletLocator = null;
//		// @formatter:off
//		/*if[accumulo.api=1.6]
//		tabletLocator = TabletLocator.getLocator(
//				(Instance) clientContextOrInstance,
//				new Text(
//						tableId));
//		else[accumulo.api=1.6]*/
//
//		tabletLocator = TabletLocator.getLocator(
//				(ClientContext) clientContextOrInstance,
//				new Text(
//						tableId));
//
//		/*end[accumulo.api=1.6]*/
//		// @formatter:on
	// return tabletLocator;
	// }
	//
	// protected static boolean binRanges(
	// final List<Range> rangeList,
	// final Object clientContextOrCredentials,
	// final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges,
	// final TabletLocator tabletLocator ) {
//		// @formatter:off
//		/*if[accumulo.api=1.6]
//		return tabletLocator.binRanges(
//				(Credentials) clientContextOrCredentials,
//				rangeList,
//				tserverBinnedRanges).isEmpty();
//  		else[accumulo.api=1.6]*/
//
//		return tabletLocator.binRanges(
//				(ClientContext) clientContextOrCredentials,
//				rangeList,
//				tserverBinnedRanges).isEmpty();
//
//  		/*end[accumulo.api=1.6]*/
//		// @formatter:on
	// }

	protected static double getRangeLength(
			final HBaseMRRowRange range ) {
		final ByteArrayId start = range.getStart();
		final ByteArrayId end = range.getEnd();

		final int maxDepth = Math.max(
				end.getBytes().length,
				start.getBytes().length);
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

}
