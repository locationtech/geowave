package mil.nga.giat.geowave.datastore.accumulo.mapreduce.input;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
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

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputConfigurator.InputConfig;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat.IntermediateSplitInfo.RangeLocationPair;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

// @formatter:off
/*if[ACCUMULO_1.5.2]
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import org.apache.accumulo.core.security.thrift.TCredentials;
end[ACCUMULO_1.5.2]*/
// @formatter:on

public class GeoWaveInputFormat<T> extends
		InputFormat<GeoWaveInputKey, T>
{
	private static final Class<?> CLASS = GeoWaveInputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);
	private static final BigInteger TWO = BigInteger.valueOf(2L);

	/**
	 * Configures a {@link AccumuloOperations} for this job.
	 * 
	 * @param config
	 *            the Hadoop configuration instance
	 * @param zooKeepers
	 *            a comma-separated list of zookeeper servers
	 * @param instanceName
	 *            the Accumulo instance name
	 * @param userName
	 *            the Accumulo user name
	 * @param password
	 *            the Accumulo password
	 * @param geowaveTableNamespace
	 *            the GeoWave table namespace
	 */
	public static void setAccumuloOperationsInfo(
			final Configuration config,
			final String zooKeepers,
			final String instanceName,
			final String userName,
			final String password,
			final String geowaveTableNamespace ) {
		GeoWaveConfiguratorBase.setAccumuloOperationsInfo(
				CLASS,
				config,
				zooKeepers,
				instanceName,
				userName,
				password,
				geowaveTableNamespace);
	}

	/**
	 * Configures a {@link AccumuloOperations} for this job.
	 * 
	 * @param job
	 *            the Hadoop job instance to be configured
	 * @param zooKeepers
	 *            a comma-separated list of zookeeper servers
	 * @param instanceName
	 *            the Accumulo instance name
	 * @param userName
	 *            the Accumulo user name
	 * @param password
	 *            the Accumulo password
	 * @param geowaveTableNamespace
	 *            the GeoWave table namespace
	 */
	public static void setAccumuloOperationsInfo(
			final Job job,
			final String zooKeepers,
			final String instanceName,
			final String userName,
			final String password,
			final String geowaveTableNamespace ) {
		setAccumuloOperationsInfo(
				job.getConfiguration(),
				zooKeepers,
				instanceName,
				userName,
				password,
				geowaveTableNamespace);
	}

	/**
	 * Add an adapter specific to the input format
	 * 
	 * @param job
	 * @param adapter
	 */
	public static void addDataAdapter(
			final Configuration config,
			final DataAdapter<?> adapter ) {

		// Also store for use the mapper and reducers
		JobContextAdapterStore.addDataAdapter(
				config,
				adapter);
		GeoWaveConfiguratorBase.addDataAdapter(
				CLASS,
				config,
				adapter);
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextIndexStore.addIndex(
				config,
				index);
	}

	public static void setMinimumSplitCount(
			final Configuration config,
			final Integer minSplits ) {
		GeoWaveInputConfigurator.setMinimumSplitCount(
				CLASS,
				config,
				minSplits);
	}

	public static void setMaximumSplitCount(
			final Configuration config,
			final Integer maxSplits ) {
		GeoWaveInputConfigurator.setMaximumSplitCount(
				CLASS,
				config,
				maxSplits);
	}

	public static void setIsOutputWritable(
			final Configuration config,
			final Boolean isOutputWritable ) {
		config.setBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				isOutputWritable);
	}

	public static void setQuery(
			final Configuration config,
			final DistributableQuery query ) {
		GeoWaveInputConfigurator.setQuery(
				CLASS,
				config,
				query);
	}

	protected static DistributableQuery getQuery(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getQuery(
				CLASS,
				context);
	}

	public static void setQueryOptions(
			final Configuration config,
			final QueryOptions queryOptions ) {
		GeoWaveInputConfigurator.setQueryOptions(
				CLASS,
				config,
				queryOptions);
	}

	protected static QueryOptions getQueryOptions(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getQueryOptions(
				CLASS,
				context);
	}

	protected static Index[] getIndices(
			final JobContext context ) {
		return GeoWaveInputConfigurator.searchForIndices(
				CLASS,
				context);
	}

	protected static String getTableNamespace(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getTableNamespace(
				CLASS,
				context);
	}

	protected static String getUserName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getUserName(
				CLASS,
				context);
	}

	protected static String getPassword(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getPassword(
				CLASS,
				context);
	}

	protected static Boolean isOutputWritable(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getConfiguration(
				context).getBoolean(
				GeoWaveConfiguratorBase.enumToConfKey(
						CLASS,
						InputConfig.OUTPUT_WRITABLE),
				false);
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
	 * @since 1.5.0
	 */
	protected static TabletLocator getTabletLocator(
			final Instance instance,
			final String tableName,
			final String tableId )
			throws TableNotFoundException {
		TabletLocator tabletLocator;
		// @formatter:off
		/*if[ACCUMULO_1.5.2]
		tabletLocator = TabletLocator.getInstance(
				instance,
				new Text(
						Tables.getTableId(
								instance,
								tableName)));

  		else[ACCUMULO_1.5.2]*/
		tabletLocator = TabletLocator.getLocator(
				instance,
				new Text(
						tableId));
		/*end[ACCUMULO_1.5.2]*/
		// @formatter:on
		return tabletLocator;
	}

	protected static boolean binRanges(
			final List<Range> rangeList,
			final String userName,
			final String password,
			final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges,
			final TabletLocator tabletLocator,
			final String instanceId )
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException,
			IOException {
		// @formatter:off
		/*if[ACCUMULO_1.5.2]
		final ByteArrayOutputStream backingByteArray = new ByteArrayOutputStream();
		final DataOutputStream output = new DataOutputStream(
				backingByteArray);
		new PasswordToken(
				password).write(output);
		output.close();
		final ByteBuffer buffer = ByteBuffer.wrap(backingByteArray.toByteArray());
		final TCredentials credentials = new TCredentials(
				userName,
				PasswordToken.class.getCanonicalName(),
				buffer,
				instanceId);
		return tabletLocator.binRanges(
				rangeList,
				tserverBinnedRanges,
				credentials).isEmpty();
  		else[ACCUMULO_1.5.2]*/
		return tabletLocator.binRanges(
				new Credentials(
						userName,
						new PasswordToken(
								password)),
				rangeList,
				tserverBinnedRanges).isEmpty();
  		/*end[ACCUMULO_1.5.2]*/
		// @formatter:on
	}

	protected static String getInstanceName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getInstanceName(
				CLASS,
				context);
	}

	protected static Integer getMinimumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMinimumSplitCount(
				CLASS,
				context);
	}

	protected static Integer getMaximumSplitCount(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getMaximumSplitCount(
				CLASS,
				context);
	}

	protected static Instance getInstance(
			final JobContext context ) {
		return GeoWaveInputConfigurator.getInstance(
				CLASS,
				context);
	}

	/**
	 * Read the metadata table to get tablets and match up ranges to them.
	 */
	@Override
	public List<InputSplit> getSplits(
			final JobContext context )
			throws IOException,
			InterruptedException {
		LOGGER.setLevel(getLogLevel(context));
		validateOptions(context);
		final Integer minSplits = getMinimumSplitCount(context);
		final Integer maxSplits = getMaximumSplitCount(context);
		final TreeSet<IntermediateSplitInfo> splits = getIntermediateSplits(
				context,
				maxSplits);
		// this is an incremental algorithm, it may be better use the target
		// split count to drive it (ie. to get 3 splits this will split 1 large
		// range into two down the middle and then split one of those ranges
		// down the middle to get 3, rather than splitting one range into
		// thirds)
		if ((minSplits != null) && (splits.size() < minSplits)) {
			// set the ranges to at least min splits
			do {
				// remove the highest range, split it into 2 and add both back,
				// increasing the size by 1
				final IntermediateSplitInfo highestSplit = splits.pollLast();
				final IntermediateSplitInfo otherSplit = highestSplit.split();
				splits.add(highestSplit);
				splits.add(otherSplit);
			}
			while (splits.size() < minSplits);
		}
		else if (((maxSplits != null) && (maxSplits > 0)) && (splits.size() > maxSplits)) {
			// merge splits to fit within max splits
			do {
				// this is the naive approach, remove the lowest two ranges and
				// merge them, decreasing the size by 1

				// TODO Ideally merge takes into account locations (as well as
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

	private static final BigInteger ONE = new BigInteger(
			"1");

	private Range getRangeMax(
			final Index index,
			final JobContext context ) {
		try {
			final AccumuloOperations operations = GeoWaveInputFormat.getAccumuloOperations(context);
			final AccumuloDataStatisticsStore store = new AccumuloDataStatisticsStore(
					operations);
			final RowRangeDataStatistics<?> stats = (RowRangeDataStatistics<?>) store.getDataStatistics(
					null,
					RowRangeDataStatistics.getId(index.getId()),
					GeoWaveInputFormat.getAuthorizations(context));

			final int cardinality = Math.max(
					stats.getMin().length,
					stats.getMax().length);
			return new Range(
					new Key(
							new Text(
									this.getKeyFromBigInteger(
											new BigInteger(
													stats.getMin()).subtract(ONE),
											cardinality))),
					true,
					new Key(
							new Text(
									this.getKeyFromBigInteger(
											new BigInteger(
													stats.getMax()).add(ONE),
											cardinality))),
					true);
		}
		catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Range();
	}

	private TreeSet<IntermediateSplitInfo> getIntermediateSplits(
			final JobContext context,
			final Integer maxSplits )
			throws IOException {
		final Index[] indices = getIndices(context);
		final DistributableQuery query = getQuery(context);
		final String tableNamespace = getTableNamespace(context);

		final TreeSet<IntermediateSplitInfo> splits = new TreeSet<IntermediateSplitInfo>();
		for (final Index index : indices) {
			if ((query != null) && !query.isSupported(index)) {
				continue;
			}
			final String tableName = AccumuloUtils.getQualifiedTableName(
					tableNamespace,
					index.getId().getString());
			final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
			final TreeSet<Range> ranges;
			if (query != null) {
				final MultiDimensionalNumericData indexConstraints = query.getIndexConstraints(indexStrategy);
				if ((maxSplits != null) && (maxSplits > 0)) {
					ranges = AccumuloUtils.byteArrayRangesToAccumuloRanges(AccumuloUtils.constraintsToByteArrayRanges(
							indexConstraints,
							indexStrategy,
							maxSplits));
				}
				else {
					ranges = AccumuloUtils.byteArrayRangesToAccumuloRanges(AccumuloUtils.constraintsToByteArrayRanges(
							indexConstraints,
							indexStrategy));
				}
			}
			else {
				ranges = new TreeSet<Range>();
				final Range fullrange = getRangeMax(
						index,
						context);
				ranges.add(fullrange);
				if (LOGGER.isTraceEnabled()) LOGGER.trace("Protected range: " + fullrange);
			}
			// get the metadata information for these ranges
			final Map<String, Map<KeyExtent, List<Range>>> tserverBinnedRanges = new HashMap<String, Map<KeyExtent, List<Range>>>();
			TabletLocator tl;
			try {
				final Instance instance = getInstance(context);
				final String tableId = Tables.getTableId(
						instance,
						tableName);
				tl = getTabletLocator(
						instance,
						tableName,
						tableId);
				// its possible that the cache could contain complete, but
				// old information about a tables tablets... so clear it
				tl.invalidateCache();
				final String instanceId = instance.getInstanceID();
				final List<Range> rangeList = new ArrayList<Range>(
						ranges);
				Random r = new Random();
				while (!binRanges(
						rangeList,
						getUserName(context),
						getPassword(context),
						tserverBinnedRanges,
						tl,
						instanceId)) {
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
					final Map<Index, List<RangeLocationPair>> splitInfo = new HashMap<Index, List<RangeLocationPair>>();
					final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();
					for (final Range range : extentRanges.getValue()) {
						rangeList.add(new RangeLocationPair(
								keyExtent.clip(range),
								location));
						if (LOGGER.isTraceEnabled()) LOGGER.warn("Clipped range: " + rangeList.get(rangeList.size() - 1).range);
					}
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

	protected static class IntermediateSplitInfo implements
			Comparable<IntermediateSplitInfo>
	{
		protected static class IndexRangeLocation
		{
			private final RangeLocationPair rangeLocationPair;
			private final Index index;

			public IndexRangeLocation(
					final RangeLocationPair rangeLocationPair,
					final Index index ) {
				this.rangeLocationPair = rangeLocationPair;
				this.index = index;
			}
		}

		protected static class RangeLocationPair
		{
			private final Range range;
			private final String location;
			private final Map<Integer, BigInteger> rangePerCardinalityCache = new HashMap<Integer, BigInteger>();

			public RangeLocationPair(
					final Range range,
					final String location ) {
				this.location = location;
				this.range = range;
			}

			protected BigInteger getRangeAtCardinality(
					final int cardinality ) {
				final BigInteger rangeAtCardinality = rangePerCardinalityCache.get(cardinality);
				if (rangeAtCardinality != null) {
					return rangeAtCardinality;
				}
				return calcRange(cardinality);

			}

			private BigInteger calcRange(
					final int cardinality ) {
				final BigInteger r = getRange(
						range,
						cardinality);
				rangePerCardinalityCache.put(
						cardinality,
						r);
				return r;
			}
		}

		private final Map<Index, List<RangeLocationPair>> splitInfo;
		private final Map<Integer, BigInteger> totalRangePerCardinalityCache = new HashMap<Integer, BigInteger>();

		public IntermediateSplitInfo(
				final Map<Index, List<RangeLocationPair>> splitInfo ) {
			this.splitInfo = splitInfo;
		}

		private synchronized void merge(
				final IntermediateSplitInfo split ) {
			clearCache();
			for (final Entry<Index, List<RangeLocationPair>> e : split.splitInfo.entrySet()) {
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

		private synchronized IntermediateSplitInfo split() {
			final int maxCardinality = getMaxCardinality();
			final BigInteger totalRange = getTotalRangeAtCardinality(maxCardinality);

			// generically you'd want the split to be as limiting to total
			// locations as possible and then as limiting as possible to total
			// indices, but in this case split() is only called when all ranges
			// are in the same location and the same index

			// and you want it to split the ranges into two by total range
			final TreeSet<IndexRangeLocation> orderedSplits = new TreeSet<IndexRangeLocation>(
					new Comparator<IndexRangeLocation>() {

						@Override
						public int compare(
								final IndexRangeLocation o1,
								final IndexRangeLocation o2 ) {
							final BigInteger range1 = o1.rangeLocationPair.getRangeAtCardinality(maxCardinality);
							final BigInteger range2 = o2.rangeLocationPair.getRangeAtCardinality(maxCardinality);
							int retVal = range1.compareTo(range2);
							if (retVal == 0) {
								// we really want to avoid equality because
								retVal = Long.compare(
										o1.hashCode(),
										o2.hashCode());
								if (retVal == 0) {
									// what the heck, give it one last insurance
									// that they're not equal even though its
									// extremely unlikely
									retVal = Long.compare(
											o1.rangeLocationPair.rangePerCardinalityCache.hashCode(),
											o2.rangeLocationPair.rangePerCardinalityCache.hashCode());
								}
							}
							return retVal;
						}
					});
			for (final Entry<Index, List<RangeLocationPair>> ranges : splitInfo.entrySet()) {
				for (final RangeLocationPair p : ranges.getValue()) {
					orderedSplits.add(new IndexRangeLocation(
							p,
							ranges.getKey()));
				}
			}
			IndexRangeLocation pairToSplit;
			BigInteger targetRange = totalRange.divide(TWO);
			final Map<Index, List<RangeLocationPair>> otherSplitInfo = new HashMap<Index, List<RangeLocationPair>>();
			do {
				// this will get the least value at or above the target range
				final BigInteger compareRange = targetRange;
				pairToSplit = orderedSplits.ceiling(new IndexRangeLocation(
						new RangeLocationPair(
								null,
								null) {

							@Override
							protected BigInteger getRangeAtCardinality(
									final int cardinality ) {
								return compareRange;
							}

						},
						null));
				// there are no elements greater than the target, so take the
				// largest element and adjust the target
				if (pairToSplit == null) {
					final IndexRangeLocation highestRange = orderedSplits.pollLast();
					List<RangeLocationPair> rangeList = otherSplitInfo.get(highestRange.index);
					if (rangeList == null) {
						rangeList = new ArrayList<RangeLocationPair>();
						otherSplitInfo.put(
								highestRange.index,
								rangeList);
					}
					rangeList.add(highestRange.rangeLocationPair);
					targetRange = targetRange.subtract(highestRange.rangeLocationPair.getRangeAtCardinality(maxCardinality));
				}
			}
			while ((pairToSplit == null) && !orderedSplits.isEmpty());

			if (pairToSplit == null) {
				// this should never happen!
				LOGGER.error("Unable to identify splits");
				// but if it does, just take the first range off of this and
				// split it in half if this is left as empty
				clearCache();
				return splitSingleRange(maxCardinality);
			}

			// now we just carve the pair to split by the amount we are over
			// the target range
			final BigInteger currentRange = pairToSplit.rangeLocationPair.getRangeAtCardinality(maxCardinality);
			final BigInteger rangeExceeded = currentRange.subtract(targetRange);
			if (rangeExceeded.compareTo(BigInteger.ZERO) > 0) {
				// remove pair to split from ordered splits and split it to
				// attempt to match the target range, adding the appropriate
				// sides of the range to this info's ordered splits and the
				// other's splits
				orderedSplits.remove(pairToSplit);
				final BigInteger end = getEnd(
						pairToSplit.rangeLocationPair.range,
						maxCardinality);
				final byte[] splitKey = getKeyFromBigInteger(
						end.subtract(rangeExceeded),
						maxCardinality);
				List<RangeLocationPair> rangeList = otherSplitInfo.get(pairToSplit.index);
				if (rangeList == null) {
					rangeList = new ArrayList<RangeLocationPair>();
					otherSplitInfo.put(
							pairToSplit.index,
							rangeList);
				}
				rangeList.add(new RangeLocationPair(
						new Range(
								pairToSplit.rangeLocationPair.range.getStartKey(),
								pairToSplit.rangeLocationPair.range.isStartKeyInclusive(),
								new Key(
										new Text(
												splitKey)),
								false),
						pairToSplit.rangeLocationPair.location));
				orderedSplits.add(new IndexRangeLocation(
						new RangeLocationPair(
								new Range(
										new Key(
												new Text(
														splitKey)),
										true,
										pairToSplit.rangeLocationPair.range.getEndKey(),
										pairToSplit.rangeLocationPair.range.isEndKeyInclusive()),
								pairToSplit.rangeLocationPair.location),
						pairToSplit.index));
			}
			else if (orderedSplits.size() > 1) {
				// add pair to split to other split and remove it from
				// orderedSplits
				orderedSplits.remove(pairToSplit);
				List<RangeLocationPair> rangeList = otherSplitInfo.get(pairToSplit.index);
				if (rangeList == null) {
					rangeList = new ArrayList<RangeLocationPair>();
					otherSplitInfo.put(
							pairToSplit.index,
							rangeList);
				}
				rangeList.add(pairToSplit.rangeLocationPair);
			}

			// clear splitinfo and set it to ordered splits (what is left of the
			// splits that haven't been placed in the other split info)
			splitInfo.clear();
			for (final IndexRangeLocation split : orderedSplits) {
				List<RangeLocationPair> rangeList = splitInfo.get(split.index);
				if (rangeList == null) {
					rangeList = new ArrayList<RangeLocationPair>();
					splitInfo.put(
							split.index,
							rangeList);
				}
				rangeList.add(split.rangeLocationPair);
			}
			clearCache();
			return new IntermediateSplitInfo(
					otherSplitInfo);
		}

		private IntermediateSplitInfo splitSingleRange(
				final int maxCardinality ) {
			final Map<Index, List<RangeLocationPair>> otherSplitInfo = new HashMap<Index, List<RangeLocationPair>>();
			final List<RangeLocationPair> otherRangeList = new ArrayList<RangeLocationPair>();
			final Iterator<Entry<Index, List<RangeLocationPair>>> it = splitInfo.entrySet().iterator();
			while (it.hasNext()) {
				final Entry<Index, List<RangeLocationPair>> e = it.next();
				final List<RangeLocationPair> rangeList = e.getValue();
				if (!rangeList.isEmpty()) {
					final RangeLocationPair p = rangeList.remove(0);
					if (rangeList.isEmpty()) {
						if (!it.hasNext()) {
							// if this is empty now, divide the split in
							// half
							final BigInteger range = p.getRangeAtCardinality(maxCardinality);
							final BigInteger start = getStart(
									p.range,
									maxCardinality);
							final byte[] splitKey = getKeyFromBigInteger(
									start.add(range.divide(TWO)),
									maxCardinality);
							rangeList.add(new RangeLocationPair(
									new Range(
											p.range.getStartKey(),
											p.range.isStartKeyInclusive(),
											new Key(
													new Text(
															splitKey)),
											false),
									p.location));
							otherRangeList.add(new RangeLocationPair(
									new Range(
											new Key(
													new Text(
															splitKey)),
											true,
											p.range.getEndKey(),
											p.range.isEndKeyInclusive()),
									p.location));
							otherSplitInfo.put(
									e.getKey(),
									otherRangeList);
							return new IntermediateSplitInfo(
									otherSplitInfo);
						}
						else {
							// otherwise remove this entry
							it.remove();
						}
					}
					otherRangeList.add(p);
					otherSplitInfo.put(
							e.getKey(),
							otherRangeList);
					return new IntermediateSplitInfo(
							otherSplitInfo);
				}
			}
			// this can only mean there are no ranges
			LOGGER.error("Attempting to split ranges on empty range");
			return new IntermediateSplitInfo(
					otherSplitInfo);
		}

		private synchronized GeoWaveInputSplit toFinalSplit() {
			final Map<Index, List<Range>> rangesPerIndex = new HashMap<Index, List<Range>>();
			final Set<String> locations = new HashSet<String>();
			for (final Entry<Index, List<RangeLocationPair>> entry : splitInfo.entrySet()) {
				final List<Range> ranges = new ArrayList<Range>(
						entry.getValue().size());
				for (final RangeLocationPair pair : entry.getValue()) {
					locations.add(pair.location);
					ranges.add(pair.range);
				}
				rangesPerIndex.put(
						entry.getKey(),
						ranges);
			}
			return new GeoWaveInputSplit(
					rangesPerIndex,
					locations.toArray(new String[locations.size()]));
		}

		private synchronized int getMaxCardinality() {
			int maxCardinality = 1;
			for (final List<RangeLocationPair> pList : splitInfo.values()) {
				for (final RangeLocationPair p : pList) {
					maxCardinality = Math.max(
							maxCardinality,
							getMaxCardinalityFromRange(p.range));
				}
			}
			return maxCardinality;
		}

		@Override
		public int compareTo(
				final IntermediateSplitInfo o ) {
			final int maxCardinality = Math.max(
					getMaxCardinality(),
					o.getMaxCardinality());
			final BigInteger thisTotal = getTotalRangeAtCardinality(maxCardinality);
			final BigInteger otherTotal = o.getTotalRangeAtCardinality(maxCardinality);
			int retVal = thisTotal.compareTo(otherTotal);
			if (retVal == 0) {
				// because this is used by the treeset, we really want to avoid
				// equality
				retVal = Long.compare(
						hashCode(),
						o.hashCode());
				// what the heck, give it one last insurance
				// that they're not equal even though its
				// extremely unlikely
				if (retVal == 0) {
					retVal = Long.compare(
							totalRangePerCardinalityCache.hashCode(),
							o.totalRangePerCardinalityCache.hashCode());
				}
			}
			return retVal;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof IntermediateSplitInfo)) {
				return false;
			}
			return this.compareTo((IntermediateSplitInfo) obj) == 0;
		}

		@Override
		public int hashCode() {
			// think this matches the spirit of compareTo
			int mc = getMaxCardinality();
			return com.google.common.base.Objects.hashCode(
					mc,
					getTotalRangeAtCardinality(mc),
					super.hashCode());
		}

		private synchronized BigInteger getTotalRangeAtCardinality(
				final int cardinality ) {
			final BigInteger totalRange = totalRangePerCardinalityCache.get(cardinality);
			if (totalRange != null) {
				return totalRange;
			}
			return calculateTotalRangeForCardinality(cardinality);
		}

		private synchronized BigInteger calculateTotalRangeForCardinality(
				final int cardinality ) {
			BigInteger sum = BigInteger.ZERO;
			for (final List<RangeLocationPair> pairList : splitInfo.values()) {
				for (final RangeLocationPair pair : pairList) {
					sum = sum.add(pair.getRangeAtCardinality(cardinality));
				}
			}
			totalRangePerCardinalityCache.put(
					cardinality,
					sum);
			return sum;
		}

		private synchronized void clearCache() {
			totalRangePerCardinalityCache.clear();
		}
	}

	protected static int getMaxCardinalityFromRange(
			final Range range ) {
		int maxCardinality = 0;
		final Key start = range.getStartKey();
		if (start != null) {
			maxCardinality = Math.max(
					maxCardinality,
					start.getRowData().length());
		}
		final Key end = range.getEndKey();
		if (end != null) {
			maxCardinality = Math.max(
					maxCardinality,
					end.getRowData().length());
		}
		return maxCardinality;
	}

	protected static byte[] getKeyFromBigInteger(
			final BigInteger value,
			final int numBytes ) {
		final byte[] valueBytes = value.toByteArray();
		final byte[] bytes = new byte[numBytes];
		for (int i = 0; i < numBytes; i++) {
			// start from the right
			if (i < valueBytes.length) {
				bytes[bytes.length - i - 1] = valueBytes[valueBytes.length - i - 1];
			}
			else {
				// prepend anything outside of the BigInteger value with 0
				bytes[bytes.length - i - 1] = 0;
			}
		}
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

	@Override
	public RecordReader<GeoWaveInputKey, T> createRecordReader(
			final InputSplit split,
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		LOGGER.setLevel(getLogLevel(context));
		return new GeoWaveRecordReader<T>();
	}

	/**
	 * Sets the log level for this job.
	 * 
	 * @param job
	 *            the Hadoop job instance to be configured
	 * @param level
	 *            the logging level
	 * @since 1.5.0
	 */
	public static void setLogLevel(
			final Configuration config,
			final Level level ) {
		ConfiguratorBase.setLogLevel(
				CLASS,
				config,
				level);
	}

	/**
	 * Gets the log level from this configuration.
	 * 
	 * @param context
	 *            the Hadoop context for the configured job
	 * @return the log level
	 * @since 1.5.0
	 * @see #setLogLevel(Job, Level)
	 */
	protected static Level getLogLevel(
			final JobContext context ) {
		return ConfiguratorBase.getLogLevel(
				CLASS,
				GeoWaveConfiguratorBase.getConfiguration(context));
	}

	/**
	 * Check whether a configuration is fully configured to be used with an
	 * Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
	 * 
	 * @param context
	 *            the Hadoop context for the configured job
	 * @throws IOException
	 *             if the context is improperly configured
	 * @since 1.5.0
	 */
	protected static void validateOptions(
			final JobContext context )
			throws IOException {
		// the only required element is the AccumuloOperations info
		try {
			// this should attempt to use the connection info to successfully
			// connect
			if (getAccumuloOperations(context) == null) {
				LOGGER.warn("Zookeeper connection for accumulo is null");
				throw new IOException(
						"Zookeeper connection for accumulo is null");
			}
		}
		catch (final AccumuloException e) {
			LOGGER.warn(
					"Error establishing zookeeper connection for accumulo",
					e);
			throw new IOException(
					e);
		}
		catch (final AccumuloSecurityException e) {
			LOGGER.warn(
					"Security error while establishing connection to accumulo",
					e);
			throw new IOException(
					e);
		}
	}

	public static AccumuloOperations getAccumuloOperations(
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {
		return GeoWaveConfiguratorBase.getAccumuloOperations(
				CLASS,
				context);
	}

	protected static String[] getAuthorizations(
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {
		return GeoWaveInputConfigurator.getAuthorizations(
				CLASS,
				context);
	}

	protected static JobContextAdapterStore getDataAdapterStore(
			final JobContext context,
			final AccumuloOperations accumuloOperations ) {
		return new JobContextAdapterStore(
				context,
				accumuloOperations);
	}

	/**
	 * First look for input-specific adapters
	 * 
	 * @param context
	 * @param adapterStore
	 * @return
	 */
	public static List<ByteArrayId> getAdapterIds(
			final JobContext context,
			final AdapterStore adapterStore ) {
		final DataAdapter<?>[] userAdapters = GeoWaveConfiguratorBase.getDataAdapters(
				CLASS,
				context);
		if ((userAdapters == null) || (userAdapters.length <= 0)) {
			return IteratorUtils.toList(IteratorUtils.transformedIterator(
					adapterStore.getAdapters(),
					new Transformer() {

						@Override
						public Object transform(
								final Object input ) {
							if (input instanceof DataAdapter) {
								return ((DataAdapter) input).getAdapterId();
							}
							return input;
						}
					}));
		}
		else {
			final List<ByteArrayId> retVal = new ArrayList<ByteArrayId>(
					userAdapters.length);
			for (final DataAdapter<?> adapter : userAdapters) {
				retVal.add(adapter.getAdapterId());
			}
			return retVal;
		}
	}

}
