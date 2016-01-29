package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.RangeLocationPair;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
//@formatter:off
/*if[accumulo.api=1.6]
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.data.KeyExtent;
else[accumulo.api=1.6]*/
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.data.impl.KeyExtent;
/*end[accumulo.api=1.6]*/
//@formatter:on

public class AccumuloMRUtils
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloMRUtils.class);

	/**
	 * Read the metadata table to get tablets and match up ranges to them.
	 */
	public static List<InputSplit> getSplits(
			final AccumuloOperations operations,
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {

		final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache = new HashMap<PrimaryIndex, RowRangeHistogramStatistics<?>>();

		try (CloseableIterator<Index<?, ?>> indices = queryOptions.getIndices(indexStore)) {
			final TreeSet<IntermediateSplitInfo> splits = getIntermediateSplits(
					operations,
					indices,
					queryOptions.getAdapterIds(adapterStore),
					statsCache,
					adapterStore,
					statsStore,
					maxSplits,
					query,
					queryOptions.getAuthorizations());

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
			final List<InputSplit> retVal = new ArrayList<InputSplit>();
			for (final IntermediateSplitInfo split : splits) {
				retVal.add(split.toFinalSplit());
			}
			return retVal;
		}
	}

	private static final BigInteger ONE = new BigInteger(
			"1");

	private static RowRangeHistogramStatistics<?> getRangeStats(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIds,
			final AdapterStore adapterStore,
			final DataStatisticsStore store,
			final String[] authorizations )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		RowRangeHistogramStatistics<?> singleStats = null;
		for (final ByteArrayId adapterId : adapterIds) {
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

	private static Range getRangeMax(
			final Index<?, ?> index,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final String[] authorizations )
			throws AccumuloException,
			AccumuloSecurityException {

		final RowRangeDataStatistics<?> stats = (RowRangeDataStatistics<?>) statsStore.getDataStatistics(
				index.getId(),
				RowRangeDataStatistics.getId(index.getId()),
				authorizations);
		if (stats == null) {
			LOGGER.warn("Could not determine range of data from 'RowRangeDataStatistics'.  Range will not be clipped. This may result in some splits being empty.");
			return new Range();
		}

		final int cardinality = Math.max(
				stats.getMin().length,
				stats.getMax().length);
		return new Range(
				new Key(
						new Text(
								getKeyFromBigInteger(
										new BigInteger(
												stats.getMin()).subtract(ONE),
										cardinality))),
				true,
				new Key(
						new Text(
								getKeyFromBigInteger(
										new BigInteger(
												stats.getMax()).add(ONE),
										cardinality))),
				true);
	}

	private static TreeSet<IntermediateSplitInfo> getIntermediateSplits(
			final AccumuloOperations operations,
			final CloseableIterator<Index<?, ?>> indices,
			final List<ByteArrayId> adapterIds,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final DistributableQuery query,
			final String[] authorizations )
			throws IOException {

		final TreeSet<IntermediateSplitInfo> splits = new TreeSet<IntermediateSplitInfo>();

		while (indices.hasNext()) {
			final PrimaryIndex index = (PrimaryIndex) indices.next();
			if ((query != null) && !query.isSupported(index)) {
				continue;
			}
			Range fullrange;
			try {
				fullrange = getRangeMax(
						index,
						adapterStore,
						statsStore,
						authorizations);
			}
			catch (final AccumuloException e) {
				fullrange = new Range();
				LOGGER.warn(
						"Cannot ascertain the full range of the data",
						e);
			}
			catch (final AccumuloSecurityException e) {
				fullrange = new Range();
				LOGGER.warn(
						"Cannot ascertain the full range of the data",
						e);
			}

			final String tableName = AccumuloUtils.getQualifiedTableName(
					operations.getGeoWaveNamespace(),
					index.getId().getString());
			final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
			final TreeSet<Range> ranges;
			if (query != null) {
				final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(indexStrategy);
				if ((maxSplits != null) && (maxSplits > 0)) {
					ranges = AccumuloUtils.byteArrayRangesToAccumuloRanges(DataStoreUtils.constraintsToByteArrayRanges(
							indexConstraints,
							indexStrategy,
							maxSplits));
				}
				else {
					ranges = AccumuloUtils.byteArrayRangesToAccumuloRanges(DataStoreUtils.constraintsToByteArrayRanges(
							indexConstraints,
							indexStrategy,
							-1));
				}
				if (ranges.size() == 1) {
					final Range range = ranges.first();
					if (range.isInfiniteStartKey() || range.isInfiniteStopKey()) {
						ranges.remove(range);
						ranges.add(fullrange.clip(range));
					}
				}
			}
			else {

				ranges = new TreeSet<Range>();
				ranges.add(fullrange);
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("Protected range: " + fullrange);
				}
			}
			// get the metadata information for these ranges
			final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges = new HashMap<String, Map<KeyExtent, List<Range>>>();
			TabletLocator tl;
			try {
				final Instance instance = operations.getInstance();
				final String tableId = Tables.getTableId(
						instance,
						tableName);

				Credentials credentials = new Credentials(
						operations.getUsername(),
						new PasswordToken(
								operations.getPassword()));

				// @formatter:off
				/*if[accumulo.api=1.6]
				tl = getTabletLocator(
						instance,
						tableId);
				Object clientContextOrCredentials = credentials;
				else[accumulo.api=1.6]*/
				ClientContext clientContext = new ClientContext(
						instance,credentials,
						new ClientConfiguration());
				tl = getTabletLocator(
						clientContext,
						tableId);

				Object clientContextOrCredentials = clientContext;
				/*end[accumulo.api=1.6]*/
				// @formatter:on
				// its possible that the cache could contain complete, but
				// old information about a tables tablets... so clear it
				tl.invalidateCache();
				final List<Range> rangeList = new ArrayList<Range>(
						ranges);
				final Random r = new Random();
				while (!binRanges(
						rangeList,
						clientContextOrCredentials,
						tserverBinnedRanges,
						tl)) {
					if (!(instance instanceof MockInstance)) {
						if (!Tables.exists(
								instance,
								tableId)) {
							throw new TableDeletedException(
									tableId);
						}
						if (Tables.getTableState(
								instance,
								tableId) == TableState.OFFLINE) {
							throw new TableOfflineException(
									instance,
									tableId);
						}
					}
					tserverBinnedRanges.clear();
					LOGGER.warn("Unable to locate bins for specified ranges. Retrying.");
					UtilWaitThread.sleep(100 + r.nextInt(101));
					// sleep randomly between 100 and 200 ms
					tl.invalidateCache();
				}
			}
			catch (final Exception e) {
				throw new IOException(
						e);
			}

			final HashMap<String, String> hostNameCache = new HashMap<String, String>();
			for (final Entry<String, Map<KeyExtent, List<Range>>> tserverBin : tserverBinnedRanges.entrySet()) {
				final String tabletServer = tserverBin.getKey();
				final String ipAddress = tabletServer.split(
						":",
						2)[0];

				String location = hostNameCache.get(ipAddress);
				if (location == null) {
					final InetAddress inetAddress = InetAddress.getByName(ipAddress);
					location = inetAddress.getHostName();
					hostNameCache.put(
							ipAddress,
							location);
				}
				for (final Entry<KeyExtent, List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
					final Range keyExtent = extentRanges.getKey().toDataRange();
					final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo = new HashMap<PrimaryIndex, List<RangeLocationPair>>();
					final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();
					for (final Range range : extentRanges.getValue()) {

						final Range clippedRange = keyExtent.clip(range);
						final double cardinality = getCardinality(
								getHistStats(
										index,
										adapterIds,
										adapterStore,
										statsStore,
										statsCache,
										authorizations),
								clippedRange);
						if (!(fullrange.beforeStartKey(clippedRange.getEndKey()) || fullrange.afterEndKey(clippedRange.getStartKey()))) {
							rangeList.add(new RangeLocationPair(
									clippedRange,
									location,
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
		}
		return splits;
	}

	private static double getCardinality(
			final RowRangeHistogramStatistics<?> rangeStats,
			final Range range ) {
		return rangeStats == null ? getRangeLength(range) : rangeStats.cardinality(
				range.getStartKey().getRow().getBytes(),
				range.getEndKey().getRow().getBytes());
	}

	private static RowRangeHistogramStatistics<?> getHistStats(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIds,
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
						adapterIds,
						adapterStore,
						statsStore,
						authorizations);
			}
			catch (final AccumuloException e) {
				throw new IOException(
						e);
			}
			catch (final AccumuloSecurityException e) {
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

				final byte[] start = rangeLocationPair.getRange().getStartKey().getRow().getBytes();
				final byte[] end = rangeLocationPair.getRange().getEndKey().getRow().getBytes();

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
							new Range(
									rangeLocationPair.getRange().getStartKey(),
									rangeLocationPair.getRange().isStartKeyInclusive(),
									new Key(
											new Text(
													splitKey)),
									false),
							location,
							splitCardinality);

					rangeLocationPair = new RangeLocationPair(
							new Range(
									new Key(
											new Text(
													splitKey)),
									true,
									rangeLocationPair.getRange().getEndKey(),
									rangeLocationPair.getRange().isEndKeyInclusive()),
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
							return (o1.rangeLocationPair.getCardinality() - o2.rangeLocationPair.getCardinality()) < 0 ? -1 : 1;
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
					final Iterator<Entry<PrimaryIndex, List<RangeLocationPair>>> it = otherSplitInfo.entrySet().iterator();
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

		private synchronized GeoWaveAccumuloInputSplit toFinalSplit() {
			final Set<String> locations = new HashSet<String>();
			for (final Entry<PrimaryIndex, List<RangeLocationPair>> entry : splitInfo.entrySet()) {
				for (final RangeLocationPair pair : entry.getValue()) {
					locations.add(pair.getLocation());
				}
			}
			return new GeoWaveAccumuloInputSplit(
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
			final ByteSequence seq,
			final int numBytes ) {
		return extractBytes(
				seq,
				numBytes,
				false);
	}

	protected static byte[] extractBytes(
			final ByteSequence seq,
			final int numBytes,
			final boolean infiniteEndKey ) {
		final byte[] bytes = new byte[numBytes + 2];
		bytes[0] = 1;
		bytes[1] = 0;
		for (int i = 0; i < numBytes; i++) {
			if (i >= seq.length()) {
				if (infiniteEndKey) {
					// -1 is 0xff
					bytes[i + 2] = -1;
				}
				else {
					bytes[i + 2] = 0;
				}
			}
			else {
				bytes[i + 2] = seq.byteAt(i);
			}
		}
		return bytes;
	}

	protected static BigInteger getRange(
			final Range range,
			final int cardinality ) {
		return getEnd(
				range,
				cardinality).subtract(
				getStart(
						range,
						cardinality));
	}

	protected static BigInteger getStart(
			final Range range,
			final int cardinality ) {
		final Key start = range.getStartKey();
		byte[] startBytes;
		if (!range.isInfiniteStartKey() && (start != null)) {
			startBytes = extractBytes(
					start.getRowData(),
					cardinality);
		}
		else {
			startBytes = extractBytes(
					new ArrayByteSequence(
							new byte[] {}),
					cardinality);
		}
		return new BigInteger(
				startBytes);
	}

	/**
	 * Initializes an Accumulo {@link TabletLocator} based on the configuration.
	 * 
	 * @param instance
	 *            the accumulo instance
	 * @param tableName
	 *            the accumulo table name
	 * @return an Accumulo tablet locator
	 * @throws TableNotFoundException
	 *             if the table name set on the configuration doesn't exist
	 * 
	 */
	protected static TabletLocator getTabletLocator(
			final Object clientContextOrInstance,
			final String tableId )
			throws TableNotFoundException {
		TabletLocator tabletLocator = null;
		// @formatter:off
		/*if[accumulo.api=1.6]
		tabletLocator = TabletLocator.getLocator(
				(Instance) clientContextOrInstance,
				new Text(
						tableId));
		else[accumulo.api=1.6]*/

		tabletLocator = TabletLocator.getLocator(
				(ClientContext) clientContextOrInstance,
				new Text(
						tableId));

		/*end[accumulo.api=1.6]*/
		// @formatter:on
		return tabletLocator;
	}

	protected static boolean binRanges(
			final List<Range> rangeList,
			final Object clientContextOrCredentials,
			final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges,
			final TabletLocator tabletLocator )
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException,
			IOException {
		// @formatter:off
		/*if[accumulo.api=1.6]
		return tabletLocator.binRanges(
				(Credentials) clientContextOrCredentials,
				rangeList,
				tserverBinnedRanges).isEmpty();
  		else[accumulo.api=1.6]*/
		
		return tabletLocator.binRanges(
				(ClientContext) clientContextOrCredentials,
				rangeList,
				tserverBinnedRanges).isEmpty();
				
  		/*end[accumulo.api=1.6]*/
		// @formatter:on
	}

	protected static BigInteger getEnd(
			final Range range,
			final int cardinality ) {
		final Key end = range.getEndKey();
		byte[] endBytes;
		if (!range.isInfiniteStopKey() && (end != null)) {
			endBytes = extractBytes(
					end.getRowData(),
					cardinality);
		}
		else {
			endBytes = extractBytes(
					new ArrayByteSequence(
							new byte[] {}),
					cardinality,
					true);
		}

		return new BigInteger(
				endBytes);
	}

	protected static double getRangeLength(
			final Range range ) {
		final ByteSequence start = range.getStartKey().getRowData();
		final ByteSequence end = range.getEndKey().getRowData();

		final int maxDepth = Math.max(
				end.length(),
				start.length());
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
