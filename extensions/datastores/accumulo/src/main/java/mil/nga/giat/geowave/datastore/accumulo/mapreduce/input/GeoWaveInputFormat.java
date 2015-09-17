package mil.nga.giat.geowave.datastore.accumulo.mapreduce.input;

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

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputConfigurator.InputConfig;
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
import org.apache.commons.lang3.tuple.Pair;
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

		AccumuloDataStatisticsStore statsStore;
		AdapterStore adapterStore;
		try {
			final Pair<AccumuloDataStatisticsStore, JobContextAdapterStore> pair = getStores(context);
			statsStore = pair.getLeft();
			adapterStore = pair.getRight();
		}
		catch (final AccumuloException e1) {
			throw new IOException(
					"Cannot connect to statistics store",
					e1);
		}
		catch (final AccumuloSecurityException e1) {
			throw new IOException(
					"Cannot connect to statistics store",
					e1);
		}

		final Map<Index, RowRangeHistogramStatistics<?>> statsCache = new HashMap<Index, RowRangeHistogramStatistics<?>>();

		final TreeSet<IntermediateSplitInfo> splits = getIntermediateSplits(
				statsCache,
				context,
				adapterStore,
				statsStore,
				maxSplits);

		// this is an incremental algorithm, it may be better use the target
		// split count to drive it (ie. to get 3 splits this will split 1 large
		// range into two down the middle and then split one of those ranges
		// down the middle to get 3, rather than splitting one range into
		// thirds)
		if (!statsCache.isEmpty() && !splits.isEmpty() && (minSplits != null) && (splits.size() < minSplits)) {
			// set the ranges to at least min splits
			do {
				// remove the highest range, split it into 2 and add both back,
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

	private Pair<AccumuloDataStatisticsStore, JobContextAdapterStore> getStores(
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {
		final AccumuloOperations operations = GeoWaveInputFormat.getAccumuloOperations(context);
		final AccumuloDataStatisticsStore statsStore = new AccumuloDataStatisticsStore(
				operations);
		final JobContextAdapterStore adapterStore = GeoWaveInputFormat.getDataAdapterStore(
				context,
				operations);
		return Pair.of(
				statsStore,
				adapterStore);
	}

	private RowRangeHistogramStatistics<?> getRangeStats(
			final Index index,
			final AdapterStore adapterStore,
			final AccumuloDataStatisticsStore store,
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException {
		RowRangeHistogramStatistics<?> singleStats = null;
		final List<ByteArrayId> adapterIds = GeoWaveInputFormat.getAdapterIds(
				context,
				adapterStore);
		for (final ByteArrayId adapterId : adapterIds) {
			final RowRangeHistogramStatistics<?> rowStat = (RowRangeHistogramStatistics<?>) store.getDataStatistics(
					adapterId,
					RowRangeHistogramStatistics.composeId(index.getId()),
					GeoWaveInputFormat.getAuthorizations(context));
			if (singleStats == null) {
				singleStats = rowStat;
			}
			else {
				singleStats.merge(rowStat);
			}
		}

		return singleStats;
	}

	private Range getRangeMax(
			final Index index,
			final AdapterStore adapterStore,
			final AccumuloDataStatisticsStore statsStore,
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {

		final RowRangeDataStatistics<?> stats = (RowRangeDataStatistics<?>) statsStore.getDataStatistics(
				index.getId(),
				RowRangeDataStatistics.getId(index.getId()),
				GeoWaveInputFormat.getAuthorizations(context));

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

	private TreeSet<IntermediateSplitInfo> getIntermediateSplits(
			final Map<Index, RowRangeHistogramStatistics<?>> statsCache,
			final JobContext context,
			final AdapterStore adapterStore,
			final AccumuloDataStatisticsStore statsStore,
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
			Range fullrange;
			try {
				fullrange = getRangeMax(
						index,
						adapterStore,
						statsStore,
						context);
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
				ranges.add(fullrange);
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("Protected range: " + fullrange);
				}
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
				final Random r = new Random();
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

						final Range clippedRange = keyExtent.clip(range);
						final double cardinality = getCardinality(
								getHistStats(
										index,
										adapterStore,
										statsStore,
										statsCache,
										context),
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

	private double getCardinality(
			final RowRangeHistogramStatistics<?> rangeStats,
			final Range range ) {
		return rangeStats == null ? getRangeLength(range) : rangeStats.cardinality(
				range.getStartKey().getRow().getBytes(),
				range.getEndKey().getRow().getBytes());
	}

	private RowRangeHistogramStatistics<?> getHistStats(
			final Index index,
			final AdapterStore adapterStore,
			final AccumuloDataStatisticsStore statsStore,
			final Map<Index, RowRangeHistogramStatistics<?>> statsCache,
			final JobContext context )
			throws IOException {
		RowRangeHistogramStatistics<?> rangeStats = statsCache.get(index);

		if (rangeStats == null) {
			try {

				rangeStats = getRangeStats(
						index,
						adapterStore,
						statsStore,
						context);
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
			private final Index index;

			public IndexRangeLocation(
					final RangeLocationPair rangeLocationPair,
					final Index index ) {
				this.rangeLocationPair = rangeLocationPair;
				this.index = index;
			}

			public IndexRangeLocation split(
					final RowRangeHistogramStatistics<?> stats,
					final double currentCardinality,
					final double targetCardinality ) {

				if (stats == null) return null;
				
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

		private final Map<Index, List<RangeLocationPair>> splitInfo;

		public IntermediateSplitInfo(
				final Map<Index, List<RangeLocationPair>> splitInfo ) {
			this.splitInfo = splitInfo;
		}

		private synchronized void merge(
				final IntermediateSplitInfo split ) {
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

		/**
		 * Side effect: Break up this split.
		 * 
		 * Split the ranges into two
		 * 
		 * @return the new split.
		 */
		private synchronized IntermediateSplitInfo split(
				final Map<Index, RowRangeHistogramStatistics<?>> statsCache ) {
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
			for (final Entry<Index, List<RangeLocationPair>> ranges : splitInfo.entrySet()) {
				for (final RangeLocationPair p : ranges.getValue()) {
					orderedSplits.add(new IndexRangeLocation(
							p,
							ranges.getKey()));
				}
			}
			final double targetCardinality = this.getTotalRangeAtCardinality() / 2;
			double currentCardinality = 0.0;
			final Map<Index, List<RangeLocationPair>> otherSplitInfo = new HashMap<Index, List<RangeLocationPair>>();

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
					final Iterator<Entry<Index, List<RangeLocationPair>>> it = otherSplitInfo.entrySet().iterator();
					final Entry<Index, List<RangeLocationPair>> entry = it.next();
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
				final Map<Index, List<RangeLocationPair>> otherSplitInfo,
				final RangeLocationPair pair,
				final Index index ) {
			List<RangeLocationPair> list = otherSplitInfo.get(index);
			if (list == null) {
				list = new ArrayList<RangeLocationPair>();
				otherSplitInfo.put(
						index,
						list);
			}
			list.add(pair);

		}

		private synchronized GeoWaveInputSplit toFinalSplit() {
			final Set<String> locations = new HashSet<String>();
			for (final Entry<Index, List<RangeLocationPair>> entry : splitInfo.entrySet()) {
				for (final RangeLocationPair pair : entry.getValue()) {
					locations.add(pair.getLocation());
				}
			}
			return new GeoWaveInputSplit(
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
			return this.compareTo((IntermediateSplitInfo) obj) == 0;
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

	protected static double getRangeLength(
			final Range range ) {
		final ByteSequence start = range.getStartKey().getRowData();
		final ByteSequence end = range.getEndKey().getRowData();

		final int maxDepth = Math.max(
				end.length(),
				start.length());
		final BigInteger startBI = new BigInteger(
				GeoWaveInputFormat.extractBytes(
						start,
						maxDepth));
		final BigInteger endBI = new BigInteger(
				GeoWaveInputFormat.extractBytes(
						end,
						maxDepth));
		return endBI.subtract(
				startBI).doubleValue();
	}

}
