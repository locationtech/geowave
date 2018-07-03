package mil.nga.giat.geowave.datastore.accumulo.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.operations.BaseReaderParams;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.core.store.server.BasicOptionProvider;
import mil.nga.giat.geowave.core.store.server.RowMergingAdapterOptionProvider;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import mil.nga.giat.geowave.core.store.server.ServerOpHelper;
import mil.nga.giat.geowave.core.store.server.ServerSideOperations;
import mil.nga.giat.geowave.core.store.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.MergingCombiner;
import mil.nga.giat.geowave.datastore.accumulo.MergingVisibilityCombiner;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.iterators.AggregationIterator;
import mil.nga.giat.geowave.datastore.accumulo.iterators.AttributeSubsettingIterator;
import mil.nga.giat.geowave.datastore.accumulo.iterators.FixedCardinalitySkippingIterator;
import mil.nga.giat.geowave.datastore.accumulo.iterators.NumericIndexStrategyFilterIterator;
import mil.nga.giat.geowave.datastore.accumulo.iterators.QueryFilterIterator;
import mil.nga.giat.geowave.datastore.accumulo.iterators.VersionIterator;
import mil.nga.giat.geowave.datastore.accumulo.iterators.WholeRowAggregationIterator;
import mil.nga.giat.geowave.datastore.accumulo.iterators.WholeRowQueryFilterIterator;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStoreOperations;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

/**
 * This class holds all parameters necessary for establishing Accumulo
 * connections and provides basic factory methods for creating a batch scanner
 * and a batch writer
 */
public class AccumuloOperations implements
		MapReduceDataStoreOperations,
		ServerSideOperations
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloOperations.class);
	private static final int DEFAULT_NUM_THREADS = 16;
	private static final long DEFAULT_TIMEOUT_MILLIS = 1000L; // 1 second
	private static final long DEFAULT_BYTE_BUFFER_SIZE = 1048576L; // 1 MB
	private static final String DEFAULT_AUTHORIZATION = null;
	private static final String DEFAULT_TABLE_NAMESPACE = "";
	private final int numThreads;
	private final long timeoutMillis;
	private final long byteBufferSize;
	private final String authorization;
	private final String tableNamespace;
	protected Connector connector;
	private final Map<String, Long> locGrpCache;
	private long cacheTimeoutMillis;
	private String password;
	private final Map<String, Set<String>> insuredAuthorizationCache = new HashMap<>();
	private final Map<String, Set<ByteArrayId>> insuredPartitionCache = new HashMap<>();
	private final AccumuloOptions options;

	/**
	 * This is will create an Accumulo connector based on passed in connection
	 * information and credentials for convenience convenience. It will also use
	 * reasonable defaults for unspecified parameters.
	 *
	 * @param zookeeperUrl
	 *            The comma-delimited URLs for all zookeeper servers, this will
	 *            be directly used to instantiate a ZookeeperInstance
	 * @param instanceName
	 *            The zookeeper instance name, this will be directly used to
	 *            instantiate a ZookeeperInstance
	 * @param userName
	 *            The username for an account to establish an Accumulo connector
	 * @param password
	 *            The password for the account to establish an Accumulo
	 *            connector
	 * @param tableNamespace
	 *            An optional string that is prefixed to any of the table names
	 * @throws AccumuloException
	 *             Thrown if a generic exception occurs when establishing a
	 *             connector
	 * @throws AccumuloSecurityException
	 *             the credentials passed in are invalid
	 */
	public AccumuloOperations(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace,
			final AccumuloOptions options )
			throws AccumuloException,
			AccumuloSecurityException {
		this(
				null,
				tableNamespace,
				options);
		this.password = password;
		connector = ConnectorPool.getInstance().getConnector(
				zookeeperUrl,
				instanceName,
				userName,
				password);

	}

	/**
	 * This constructor uses reasonable defaults and only requires an Accumulo
	 * connector
	 *
	 * @param connector
	 *            The connector to use for all operations
	 */
	public AccumuloOperations(
			final Connector connector,
			final AccumuloOptions options ) {
		this(
				connector,
				DEFAULT_TABLE_NAMESPACE,
				options);
	}

	/**
	 * This constructor uses reasonable defaults and requires an Accumulo
	 * connector and table namespace
	 *
	 * @param connector
	 *            The connector to use for all operations
	 * @param password
	 *            An optional string that is prefixed to any of the table names
	 * @param tableNamespace
	 *            An optional string that is prefixed to any of the table names
	 */
	public AccumuloOperations(
			final Connector connector,
			final String tableNamespace,
			final AccumuloOptions options ) {
		this(
				DEFAULT_NUM_THREADS,
				DEFAULT_TIMEOUT_MILLIS,
				DEFAULT_BYTE_BUFFER_SIZE,
				DEFAULT_AUTHORIZATION,
				tableNamespace,
				connector,
				options);
	}

	/**
	 * This is the full constructor for the operation factory and should be used
	 * if any of the defaults are insufficient.
	 *
	 * @param numThreads
	 *            The number of threads to use for a batch scanner and batch
	 *            writer
	 * @param timeoutMillis
	 *            The time out in milliseconds to use for a batch writer
	 * @param byteBufferSize
	 *            The buffer size in bytes to use for a batch writer
	 * @param authorization
	 *            The authorization to use for a batch scanner
	 * @param tableNamespace
	 *            An optional string that is prefixed to any of the table names
	 * @param connector
	 *            The connector to use for all operations
	 */
	public AccumuloOperations(
			final int numThreads,
			final long timeoutMillis,
			final long byteBufferSize,
			final String authorization,
			final String tableNamespace,
			final Connector connector,
			final AccumuloOptions options ) {
		this.numThreads = numThreads;
		this.timeoutMillis = timeoutMillis;
		this.byteBufferSize = byteBufferSize;
		this.authorization = authorization;
		this.tableNamespace = tableNamespace;
		this.connector = connector;
		this.options = options;
		locGrpCache = new HashMap<String, Long>();
		cacheTimeoutMillis = TimeUnit.DAYS.toMillis(1);
	}

	public int getNumThreads() {
		return numThreads;
	}

	public long getTimeoutMillis() {
		return timeoutMillis;
	}

	public long getByteBufferSize() {
		return byteBufferSize;
	}

	public Connector getConnector() {
		return connector;
	}

	private String[] getAuthorizations(
			final String... additionalAuthorizations ) {
		final String[] safeAdditionalAuthorizations = additionalAuthorizations == null ? new String[] {}
				: additionalAuthorizations;

		return authorization == null ? safeAdditionalAuthorizations : (String[]) ArrayUtils.add(
				safeAdditionalAuthorizations,
				authorization);
	}

	public boolean createTable(
			final String tableName,
			final boolean enableVersioning,
			final boolean enableBlockCache ) {
		final String qName = getQualifiedTableName(tableName);

		if (!connector.tableOperations().exists(
				qName)) {
			try {
				final NewTableConfiguration config = new NewTableConfiguration();

				final Map<String, String> propMap = new HashMap(
						config.getProperties());

				if (enableBlockCache) {
					propMap.put(
							Property.TABLE_BLOCKCACHE_ENABLED.getKey(),
							"true");

					config.setProperties(propMap);
				}

				connector.tableOperations().create(
						qName,
						config);

				// Versioning is on by default; only need to detach
				if (!enableVersioning) {
					enableVersioningIterator(
							tableName,
							false);
				}
				return true;
			}
			catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
				LOGGER.warn(
						"Unable to create table '" + qName + "'",
						e);
			}
			catch (final TableNotFoundException e) {
				LOGGER.error(
						"Error disabling version iterator",
						e);
			}
		}
		return false;
	}

	public long getRowCount(
			final String tableName,
			final String... additionalAuthorizations ) {
		RowIterator rowIterator;
		try {
			rowIterator = new RowIterator(
					connector.createScanner(
							getQualifiedTableName(tableName),
							(authorization == null) ? new Authorizations(
									additionalAuthorizations) : new Authorizations(
									(String[]) ArrayUtils.add(
											additionalAuthorizations,
											authorization))));
			while (rowIterator.hasNext()) {
				rowIterator.next();
			}
			return rowIterator.getKVCount();
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Table '" + tableName + "' not found during count operation",
					e);
			return 0;
		}
	}

	public boolean deleteTable(
			final String tableName ) {
		final String qName = getQualifiedTableName(tableName);
		try {
			connector.tableOperations().delete(
					qName);
			return true;
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to delete table, table not found '" + qName + "'",
					e);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to delete table '" + qName + "'",
					e);
		}
		return false;
	}

	public String getTableNameSpace() {
		return tableNamespace;
	}

	private String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return AccumuloUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

	/**
	 *
	 */
	@Override
	public void deleteAll()
			throws Exception {
		SortedSet<String> tableNames = connector.tableOperations().list();

		if ((tableNamespace != null) && !tableNamespace.isEmpty()) {
			tableNames = tableNames.subSet(
					tableNamespace,
					tableNamespace + '\uffff');
		}

		for (final String tableName : tableNames) {
			connector.tableOperations().delete(
					tableName);
		}
		DataAdapterAndIndexCache.getInstance(
				RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID,
				tableNamespace,
				AccumuloStoreFactoryFamily.TYPE).deleteAll();
		locGrpCache.clear();
		insuredAuthorizationCache.clear();
		insuredPartitionCache.clear();
	}

	public boolean delete(
			final String tableName,
			final ByteArrayId rowId,
			final String columnFamily,
			final byte[] columnQualifier,
			final String... additionalAuthorizations ) {
		return this.delete(
				tableName,
				Arrays.asList(rowId),
				columnFamily,
				columnQualifier,
				additionalAuthorizations);
	}

	public boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		BatchDeleter deleter = null;
		try {
			deleter = createBatchDeleter(
					tableName,
					additionalAuthorizations);
			deleter.setRanges(Arrays.asList(new Range()));
			deleter.fetchColumnFamily(new Text(
					columnFamily));
			deleter.delete();
			return true;
		}
		catch (final TableNotFoundException | MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to delete row from table [" + tableName + "].",
					e);
			return false;
		}
		finally {
			if (deleter != null) {
				deleter.close();
			}
		}

	}

	public boolean delete(
			final String tableName,
			final List<ByteArrayId> rowIds,
			final String columnFamily,
			final byte[] columnQualifier,
			final String... authorizations ) {

		boolean success = true;
		BatchDeleter deleter = null;
		try {
			deleter = createBatchDeleter(
					tableName,
					authorizations);
			if ((columnFamily != null) && !columnFamily.isEmpty()) {
				if ((columnQualifier != null) && columnQualifier.length != 0) {
					deleter.fetchColumn(
							new Text(
									columnFamily),
							new Text(
									columnQualifier));
				}
				else {
					deleter.fetchColumnFamily(new Text(
							columnFamily));
				}
			}
			final Set<ByteArrayId> removeSet = new HashSet<ByteArrayId>();
			final List<Range> rowRanges = new ArrayList<Range>();
			for (final ByteArrayId rowId : rowIds) {
				rowRanges.add(Range.exact(new Text(
						rowId.getBytes())));
				removeSet.add(new ByteArrayId(
						rowId.getBytes()));
			}
			deleter.setRanges(rowRanges);

			final Iterator<Map.Entry<Key, Value>> iterator = deleter.iterator();
			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				removeSet.remove(new ByteArrayId(
						entry.getKey().getRowData().getBackingArray()));
			}

			if (removeSet.isEmpty()) {
				deleter.delete();
			}

			deleter.close();
		}
		catch (final TableNotFoundException | MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to delete row from table [" + tableName + "].",
					e);
			if (deleter != null) {
				deleter.close();
			}
			success = false;
		}

		return success;
	}

	public boolean localityGroupExists(
			final String tableName,
			final byte[] localityGroup )
			throws AccumuloException,
			TableNotFoundException {
		final String qName = getQualifiedTableName(tableName);
		final String localityGroupStr = qName + StringUtils.stringFromBinary(localityGroup);

		// check the cache for our locality group
		if (locGrpCache.containsKey(localityGroupStr)) {
			if ((locGrpCache.get(localityGroupStr) - new Date().getTime()) < cacheTimeoutMillis) {
				return true;
			}
			else {
				locGrpCache.remove(localityGroupStr);
			}
		}

		// check accumulo to see if locality group exists
		final boolean groupExists = connector.tableOperations().exists(
				qName) && connector.tableOperations().getLocalityGroups(
				qName).keySet().contains(
				StringUtils.stringFromBinary(localityGroup));

		// update the cache
		if (groupExists) {
			locGrpCache.put(
					localityGroupStr,
					new Date().getTime());
		}

		return groupExists;
	}

	public void addLocalityGroup(
			final String tableName,
			final byte[] localityGroup )
			throws AccumuloException,
			TableNotFoundException,
			AccumuloSecurityException {
		final String qName = getQualifiedTableName(tableName);
		final String localityGroupStr = qName + StringUtils.stringFromBinary(localityGroup);

		// check the cache for our locality group
		if (locGrpCache.containsKey(localityGroupStr)) {
			if ((locGrpCache.get(localityGroupStr) - new Date().getTime()) < cacheTimeoutMillis) {
				return;
			}
			else {
				locGrpCache.remove(localityGroupStr);
			}
		}

		// add locality group to accumulo and update the cache
		if (connector.tableOperations().exists(
				qName)) {
			final Map<String, Set<Text>> localityGroups = connector.tableOperations().getLocalityGroups(
					qName);

			final Set<Text> groupSet = new HashSet<Text>();

			groupSet.add(new Text(
					localityGroup));

			localityGroups.put(
					StringUtils.stringFromBinary(localityGroup),
					groupSet);

			connector.tableOperations().setLocalityGroups(
					qName,
					localityGroups);

			locGrpCache.put(
					localityGroupStr,
					new Date().getTime());
		}
	}

	public ClientSideIteratorScanner createClientScanner(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException {
		return new ClientSideIteratorScanner(
				createScanner(
						tableName,
						additionalAuthorizations));
	}

	public Scanner createScanner(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException {
		return connector.createScanner(
				getQualifiedTableName(tableName),
				new Authorizations(
						getAuthorizations(additionalAuthorizations)));
	}

	public BatchScanner createBatchScanner(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException {
		return connector.createBatchScanner(
				getQualifiedTableName(tableName),
				new Authorizations(
						getAuthorizations(additionalAuthorizations)),
				numThreads);
	}

	@Override
	public boolean insureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		String user;
		if (clientUser == null) {
			user = connector.whoami();
		}
		else {
			user = clientUser;
		}
		final Set<String> uninsuredAuths = new HashSet<String>();
		Set<String> insuredAuths = insuredAuthorizationCache.get(user);
		if (insuredAuths == null) {
			uninsuredAuths.addAll(Arrays.asList(authorizations));
			insuredAuths = new HashSet<String>();
			insuredAuthorizationCache.put(
					user,
					insuredAuths);
		}
		else {
			for (final String auth : authorizations) {
				if (!insuredAuths.contains(auth)) {
					uninsuredAuths.add(auth);
				}
			}
		}
		if (!uninsuredAuths.isEmpty()) {
			try {
				Authorizations auths = connector.securityOperations().getUserAuthorizations(
						user);
				final List<byte[]> newSet = new ArrayList<byte[]>();
				for (final String auth : uninsuredAuths) {
					if (!auths.contains(auth)) {
						newSet.add(auth.getBytes(StringUtils.UTF8_CHAR_SET));
					}
				}
				if (newSet.size() > 0) {
					newSet.addAll(auths.getAuthorizations());
					connector.securityOperations().changeUserAuthorizations(
							user,
							new Authorizations(
									newSet));
					auths = connector.securityOperations().getUserAuthorizations(
							user);

					LOGGER.trace(clientUser + " has authorizations " + ArrayUtils.toString(auths.getAuthorizations()));
				}
				for (final String auth : uninsuredAuths) {
					insuredAuths.add(auth);
				}
			}
			catch (AccumuloException | AccumuloSecurityException e) {
				LOGGER.error(
						"Unable to add authorizations '" + Arrays.toString(uninsuredAuths.toArray(new String[] {}))
								+ "'",
						e);
				return false;
			}
		}
		return true;
	}

	public BatchDeleter createBatchDeleter(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException {
		return connector.createBatchDeleter(
				getQualifiedTableName(tableName),
				new Authorizations(
						getAuthorizations(additionalAuthorizations)),
				numThreads,
				new BatchWriterConfig().setMaxWriteThreads(
						numThreads).setMaxMemory(
						byteBufferSize).setTimeout(
						timeoutMillis,
						TimeUnit.MILLISECONDS));
	}

	public long getCacheTimeoutMillis() {
		return cacheTimeoutMillis;
	}

	public void setCacheTimeoutMillis(
			final long cacheTimeoutMillis ) {
		this.cacheTimeoutMillis = cacheTimeoutMillis;
	}

	public void insurePartition(
			final ByteArrayId partition,
			final String tableName ) {
		final String qName = getQualifiedTableName(tableName);
		Set<ByteArrayId> existingPartitions = insuredPartitionCache.get(qName);
		try {
			synchronized (insuredPartitionCache) {
				if (existingPartitions == null) {
					Collection<Text> splits;
					splits = connector.tableOperations().listSplits(
							qName);
					existingPartitions = new HashSet<>();
					for (final Text s : splits) {
						existingPartitions.add(new ByteArrayId(
								s.getBytes()));
					}
					insuredPartitionCache.put(
							qName,
							existingPartitions);
				}
				if (!existingPartitions.contains(partition)) {
					final SortedSet<Text> partitionKeys = new TreeSet<Text>();
					partitionKeys.add(new Text(
							partition.getBytes()));
					connector.tableOperations().addSplits(
							qName,
							partitionKeys);
					existingPartitions.add(partition);
				}
			}
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
			LOGGER.warn(
					"Unable to add partition '" + partition.getHexString() + "' to table '" + qName + "'",
					e);
		}
	}

	public boolean attachIterators(
			final String tableName,
			final boolean createTable,
			final boolean enableVersioning,
			final boolean enableBlockCache,
			final IteratorConfig... iterators )
			throws TableNotFoundException {
		final String qName = getQualifiedTableName(tableName);
		if (createTable && !connector.tableOperations().exists(
				qName)) {
			createTable(
					tableName,
					enableVersioning,
					enableBlockCache);
		}
		try {
			if ((iterators != null) && (iterators.length > 0)) {
				final Map<String, EnumSet<IteratorScope>> iteratorScopes = connector.tableOperations().listIterators(
						qName);
				for (final IteratorConfig iteratorConfig : iterators) {
					boolean mustDelete = false;
					boolean exists = false;
					final EnumSet<IteratorScope> existingScopes = iteratorScopes.get(iteratorConfig.getIteratorName());
					EnumSet<IteratorScope> configuredScopes;
					if (iteratorConfig.getScopes() == null) {
						configuredScopes = EnumSet.allOf(IteratorScope.class);
					}
					else {
						configuredScopes = iteratorConfig.getScopes();
					}
					Map<String, String> configuredOptions = null;
					if (existingScopes != null) {
						if (existingScopes.size() == configuredScopes.size()) {
							exists = true;
							for (final IteratorScope s : existingScopes) {
								if (!configuredScopes.contains(s)) {
									// this iterator exists with the wrong
									// scope, we will assume we want to remove
									// it and add the new configuration
									LOGGER.warn("found iterator '" + iteratorConfig.getIteratorName()
											+ "' missing scope '" + s.name() + "', removing it and re-attaching");

									mustDelete = true;
									break;
								}
							}
						}
						if (existingScopes.size() > 0) {
							// see if the options are the same, if they are not
							// the same, apply a merge with the existing options
							// and the configured options
							final Iterator<IteratorScope> it = existingScopes.iterator();
							while (it.hasNext()) {
								final IteratorScope scope = it.next();
								final IteratorSetting setting = connector.tableOperations().getIteratorSetting(
										qName,
										iteratorConfig.getIteratorName(),
										scope);
								if (setting != null) {
									final Map<String, String> existingOptions = setting.getOptions();
									configuredOptions = iteratorConfig.getOptions(existingOptions);
									if (existingOptions == null) {
										mustDelete = (configuredOptions == null);
									}
									else if (configuredOptions == null) {
										mustDelete = true;
									}
									else {
										// neither are null, compare the size of
										// the entry sets and check that they
										// are equivalent
										final Set<Entry<String, String>> existingEntries = existingOptions.entrySet();
										final Set<Entry<String, String>> configuredEntries = configuredOptions
												.entrySet();
										if (existingEntries.size() != configuredEntries.size()) {
											mustDelete = true;
										}
										else {
											mustDelete = (!existingEntries.containsAll(configuredEntries));
										}
									}
									// we found the setting existing in one
									// scope, assume the options are the same
									// for each scope
									break;
								}
							}
						}
					}
					if (mustDelete) {
						connector.tableOperations().removeIterator(
								qName,
								iteratorConfig.getIteratorName(),
								existingScopes);
						exists = false;
					}
					if (!exists) {
						if (configuredOptions == null) {
							configuredOptions = iteratorConfig.getOptions(new HashMap<String, String>());
						}
						connector.tableOperations().attachIterator(
								qName,
								new IteratorSetting(
										iteratorConfig.getIteratorPriority(),
										iteratorConfig.getIteratorName(),
										iteratorConfig.getIteratorClass(),
										configuredOptions),
								configuredScopes);
					}
				}
			}
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.warn(
					"Unable to create table '" + qName + "'",
					e);
		}
		return true;
	}

	public static AccumuloOperations createOperations(
			final AccumuloRequiredOptions options )
			throws AccumuloException,
			AccumuloSecurityException {
		return new AccumuloOperations(
				options.getZookeeper(),
				options.getInstance(),
				options.getUser(),
				options.getPassword(),
				options.getGeowaveNamespace(),
				(AccumuloOptions) options.getStoreOptions());
	}

	public static Connector getConnector(
			final AccumuloRequiredOptions options )
			throws AccumuloException,
			AccumuloSecurityException {
		return createOperations(options).connector;
	}

	public static String getUsername(
			final AccumuloRequiredOptions options )
			throws AccumuloException,
			AccumuloSecurityException {
		return options.getUser();
	}

	public static String getPassword(
			final AccumuloRequiredOptions options )
			throws AccumuloException,
			AccumuloSecurityException {
		return options.getPassword();
	}

	public String getGeoWaveNamespace() {
		return tableNamespace;
	}

	public String getUsername() {
		return connector.whoami();
	}

	public String getPassword() {
		return password;
	}

	public Instance getInstance() {
		return connector.getInstance();
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException {
		final String qName = getQualifiedTableName(indexId.getString());
		return connector.tableOperations().exists(
				qName);
	}

	@Override
	public boolean deleteAll(
			final ByteArrayId indexId,
			final Short adapterId,
			final String... additionalAuthorizations ) {
		BatchDeleter deleter = null;
		try {
			deleter = createBatchDeleter(
					indexId.getString(),
					additionalAuthorizations);

			deleter.setRanges(Arrays.asList(new Range()));
			deleter.fetchColumnFamily(new Text(
					ByteArrayUtils.shortToString(adapterId)));
			deleter.delete();
			return true;
		}
		catch (final TableNotFoundException | MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to delete row from table [" + indexId.getString() + "].",
					e);
			return false;
		}
		finally {
			if (deleter != null) {
				deleter.close();
			}
		}

	}

	protected ScannerBase getScanner(
			final ReaderParams params ) {
		final List<ByteArrayRange> ranges = params.getQueryRanges().getCompositeQueryRanges();
		final String tableName = StringUtils.stringFromBinary(params.getIndex().getId().getBytes());
		ScannerBase scanner;
		try {
			if (!params.isAggregation() && (ranges != null) && (ranges.size() == 1)) {
				if (!options.isServerSideLibraryEnabled()) {
					scanner = createClientScanner(
							tableName,
							params.getAdditionalAuthorizations());
				}
				else {
					scanner = createScanner(
							tableName,
							params.getAdditionalAuthorizations());
				}
				final ByteArrayRange r = ranges.get(0);
				if (r.isSingleValue()) {
					((Scanner) scanner).setRange(Range.exact(new Text(
							r.getStart().getBytes())));
				}
				else {
					((Scanner) scanner).setRange(AccumuloUtils.byteArrayRangeToAccumuloRange(r));
				}
				if ((params.getLimit() != null) && (params.getLimit() > 0)
						&& (params.getLimit() < ((Scanner) scanner).getBatchSize())) {
					// do allow the limit to be set to some enormous size.
					((Scanner) scanner).setBatchSize(Math.min(
							1024,
							params.getLimit()));
				}
			}
			else {
				scanner = createBatchScanner(
						tableName,
						params.getAdditionalAuthorizations());
				((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(ranges));
			}
			if (params.getMaxResolutionSubsamplingPerDimension() != null) {
				if (params.getMaxResolutionSubsamplingPerDimension().length != params
						.getIndex()
						.getIndexStrategy()
						.getOrderedDimensionDefinitions().length) {
					LOGGER.warn("Unable to subsample for table '" + tableName + "'. Subsample dimensions = "
							+ params.getMaxResolutionSubsamplingPerDimension().length + " when indexed dimensions = "
							+ params.getIndex().getIndexStrategy().getOrderedDimensionDefinitions().length);
				}
				else {

					final int cardinalityToSubsample = (int) Math.round(IndexUtils.getDimensionalBitsUsed(
							params.getIndex().getIndexStrategy(),
							params.getMaxResolutionSubsamplingPerDimension())
							+ (8 * params.getIndex().getIndexStrategy().getPartitionKeyLength()));

					final IteratorSetting iteratorSettings = new IteratorSetting(
							FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_PRIORITY,
							FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_NAME,
							FixedCardinalitySkippingIterator.class);
					iteratorSettings.addOption(
							FixedCardinalitySkippingIterator.CARDINALITY_SKIP_INTERVAL,
							Integer.toString(cardinalityToSubsample));
					scanner.addScanIterator(iteratorSettings);
				}
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
			return null;
		}
		if ((params.getAdapterIds() != null) && !params.getAdapterIds().isEmpty()) {
			for (final short adapterId : params.getAdapterIds()) {
				scanner.fetchColumnFamily(new Text(
						ByteArrayUtils.shortToString(adapterId)));
			}
		}
		return scanner;
	}

	protected void addConstraintsScanIteratorSettings(
			final BaseReaderParams params,
			final ScannerBase scanner,
			final DataStoreOptions options ) {
		addFieldSubsettingToIterator(
				params,
				scanner);
		IteratorSetting iteratorSettings = null;
		if (params.isServersideAggregation()) {
			if (params.isMixedVisibility()) {
				iteratorSettings = new IteratorSetting(
						QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
						QueryFilterIterator.QUERY_ITERATOR_NAME,
						WholeRowAggregationIterator.class);
			}
			else {
				iteratorSettings = new IteratorSetting(
						QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
						QueryFilterIterator.QUERY_ITERATOR_NAME,
						AggregationIterator.class);
			}
			if (!(params.getAggregation().getRight() instanceof CommonIndexAggregation)
					&& (params.getAggregation().getLeft() != null)) {
				iteratorSettings.addOption(
						AggregationIterator.ADAPTER_OPTION_NAME,
						ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary((DataAdapter<?>) params
								.getAggregation()
								.getLeft())));
			}
			final Aggregation aggr = (Aggregation) params.getAggregation().getRight();
			iteratorSettings.addOption(
					AggregationIterator.AGGREGATION_OPTION_NAME,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toClassId(aggr)));
			if (aggr.getParameters() != null) { // sets the parameters
				iteratorSettings.addOption(
						AggregationIterator.PARAMETER_OPTION_NAME,
						ByteArrayUtils.byteArrayToString((PersistenceUtils.toBinary(aggr.getParameters()))));
			}
			if ((params.getConstraints() != null) && !params.getConstraints().isEmpty()) {
				iteratorSettings.addOption(
						AggregationIterator.CONSTRAINTS_OPTION_NAME,
						ByteArrayUtils.byteArrayToString((PersistenceUtils.toBinary(params.getConstraints()))));
			}
			iteratorSettings.addOption(
					AggregationIterator.INDEX_STRATEGY_OPTION_NAME,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(params.getIndex().getIndexStrategy())));
			// the index model and partition key length must be provided for the
			// aggregation iterator to deserialize each entry

			iteratorSettings.addOption(
					QueryFilterIterator.PARTITION_KEY_LENGTH,
					Integer.toString(params.getIndex().getIndexStrategy().getPartitionKeyLength()));
			iteratorSettings.addOption(
					QueryFilterIterator.MODEL,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(params.getIndex().getIndexModel())));
			// don't bother setting max decomposition because it is just the
			// default anyways
		}

		boolean usingDistributableFilter = false;

		if (params.getFilter() != null) {
			usingDistributableFilter = true;
			if (iteratorSettings == null) {
				if (params.isMixedVisibility()) {
					iteratorSettings = new IteratorSetting(
							QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
							QueryFilterIterator.QUERY_ITERATOR_NAME,
							WholeRowQueryFilterIterator.class);
				}
				else {
					iteratorSettings = new IteratorSetting(
							QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
							QueryFilterIterator.QUERY_ITERATOR_NAME,
							QueryFilterIterator.class);
				}
			}
			iteratorSettings.addOption(
					QueryFilterIterator.FILTER,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(params.getFilter())));
			if (!iteratorSettings.getOptions().containsKey(
					QueryFilterIterator.MODEL)) {
				// it may already be added as an option if its an aggregation
				iteratorSettings.addOption(
						QueryFilterIterator.MODEL,
						ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(params.getIndex().getIndexModel())));
				iteratorSettings.addOption(
						QueryFilterIterator.PARTITION_KEY_LENGTH,
						Integer.toString(params.getIndex().getIndexStrategy().getPartitionKeyLength()));
			}
		}
		else if ((iteratorSettings == null) && params.isMixedVisibility()) {
			// we have to at least use a whole row iterator
			iteratorSettings = new IteratorSetting(
					QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
					QueryFilterIterator.QUERY_ITERATOR_NAME,
					WholeRowIterator.class);
		}
		if (!usingDistributableFilter) {
			// it ends up being duplicative and slower to add both a
			// distributable query and the index constraints, but one of the two
			// is important to limit client-side filtering
			addIndexFilterToIterator(
					params,
					scanner);
		}
		if (iteratorSettings != null) {
			scanner.addScanIterator(iteratorSettings);
		}
	}

	protected void addIndexFilterToIterator(
			final BaseReaderParams params,
			final ScannerBase scanner ) {
		final List<MultiDimensionalCoordinateRangesArray> coords = params.getCoordinateRanges();
		if ((coords != null) && !coords.isEmpty()) {
			final IteratorSetting iteratorSetting = new IteratorSetting(
					NumericIndexStrategyFilterIterator.IDX_FILTER_ITERATOR_PRIORITY,
					NumericIndexStrategyFilterIterator.IDX_FILTER_ITERATOR_NAME,
					NumericIndexStrategyFilterIterator.class);

			iteratorSetting.addOption(
					NumericIndexStrategyFilterIterator.INDEX_STRATEGY_KEY,
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(params.getIndex().getIndexStrategy())));

			iteratorSetting.addOption(
					NumericIndexStrategyFilterIterator.COORDINATE_RANGE_KEY,
					ByteArrayUtils.byteArrayToString(new ArrayOfArrays(
							coords.toArray(new MultiDimensionalCoordinateRangesArray[] {})).toBinary()));
			scanner.addScanIterator(iteratorSetting);
		}
	}

	protected void addFieldSubsettingToIterator(
			final BaseReaderParams params,
			final ScannerBase scanner ) {
		if ((params.getFieldSubsets() != null) && !params.isAggregation()) {
			final List<String> fieldIds = (List<String>) params.getFieldSubsets().getLeft();
			final DataAdapter<?> associatedAdapter = (DataAdapter<?>) params.getFieldSubsets().getRight();
			if ((fieldIds != null) && (!fieldIds.isEmpty()) && (associatedAdapter != null)) {
				final IteratorSetting iteratorSetting = AttributeSubsettingIterator.getIteratorSetting();

				AttributeSubsettingIterator.setFieldIds(
						iteratorSetting,
						associatedAdapter,
						fieldIds,
						params.getIndex().getIndexModel());

				iteratorSetting.addOption(
						AttributeSubsettingIterator.WHOLE_ROW_ENCODED_KEY,
						Boolean.toString(params.isMixedVisibility()));
				scanner.addScanIterator(iteratorSetting);
			}
		}
	}

	protected void addRowScanIteratorSettings(
			final ReaderParams params,
			final ScannerBase scanner ) {
		addFieldSubsettingToIterator(
				params,
				scanner);
		if (params.isMixedVisibility()) {
			// we have to at least use a whole row iterator
			final IteratorSetting iteratorSettings = new IteratorSetting(
					QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
					QueryFilterIterator.QUERY_ITERATOR_NAME,
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);
		}
	}

	@Override
	public Reader createReader(
			final ReaderParams params ) {
		final ScannerBase scanner = getScanner(params);

		addConstraintsScanIteratorSettings(
				params,
				scanner,
				options);

		return new AccumuloReader(
				scanner,
				params.getIndex().getIndexStrategy().getPartitionKeyLength(),
				params.isMixedVisibility() && !params.isServersideAggregation(),
				params.isClientsideRowMerging());
	}

	protected Scanner getScanner(
			final RecordReaderParams params ) {
		final GeoWaveRowRange range = params.getRowRange();
		final String tableName = StringUtils.stringFromBinary(params.getIndex().getId().getBytes());
		Scanner scanner;
		try {
			scanner = createScanner(
					tableName,
					params.getAdditionalAuthorizations());
			if (range == null) {
				scanner.setRange(new Range());
			}
			else {
				scanner.setRange(AccumuloSplitsProvider.toAccumuloRange(
						range,
						params.getIndex().getIndexStrategy().getPartitionKeyLength()));
			}
			if ((params.getLimit() != null) && (params.getLimit() > 0) && (params.getLimit() < scanner.getBatchSize())) {
				// do allow the limit to be set to some enormous size.
				scanner.setBatchSize(Math.min(
						1024,
						params.getLimit()));
			}
			if (params.getMaxResolutionSubsamplingPerDimension() != null) {
				if (params.getMaxResolutionSubsamplingPerDimension().length != params
						.getIndex()
						.getIndexStrategy()
						.getOrderedDimensionDefinitions().length) {
					LOGGER.warn("Unable to subsample for table '" + tableName + "'. Subsample dimensions = "
							+ params.getMaxResolutionSubsamplingPerDimension().length + " when indexed dimensions = "
							+ params.getIndex().getIndexStrategy().getOrderedDimensionDefinitions().length);
				}
				else {

					final int cardinalityToSubsample = (int) Math.round(IndexUtils.getDimensionalBitsUsed(
							params.getIndex().getIndexStrategy(),
							params.getMaxResolutionSubsamplingPerDimension())
							+ (8 * params.getIndex().getIndexStrategy().getPartitionKeyLength()));

					final IteratorSetting iteratorSettings = new IteratorSetting(
							FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_PRIORITY,
							FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_NAME,
							FixedCardinalitySkippingIterator.class);
					iteratorSettings.addOption(
							FixedCardinalitySkippingIterator.CARDINALITY_SKIP_INTERVAL,
							Integer.toString(cardinalityToSubsample));
					scanner.addScanIterator(iteratorSettings);
				}
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
			return null;
		}
		if ((params.getAdapterIds() != null) && !params.getAdapterIds().isEmpty()) {
			for (final Short adapterId : params.getAdapterIds()) {
				scanner.fetchColumnFamily(new Text(
						ByteArrayUtils.shortToString(adapterId)));
			}
		}
		return scanner;
	}

	@Override
	public Reader createReader(
			final RecordReaderParams readerParams ) {
		final ScannerBase scanner = getScanner(readerParams);
		addConstraintsScanIteratorSettings(
				readerParams,
				scanner,
				options);
		return new AccumuloReader(
				scanner,
				readerParams.getIndex().getIndexStrategy().getPartitionKeyLength(),
				readerParams.isMixedVisibility() && !readerParams.isServersideAggregation(),
				false);
	}

	@Override
	public Deleter createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception {
		return new AccumuloDeleter(
				createBatchDeleter(
						indexId.getString(),
						authorizations),
				indexId.getString().endsWith(
						"ALT_INDEX_TABLE")); // TODO: GEOWAVE-1018, incorporate
												// more robust secondary index
												// deletion/bookkeeping methods
	}

	@Override
	public Writer createWriter(
			final ByteArrayId indexId,
			final short internalAdapterId ) {
		final String tableName = indexId.getString();
		if (options.isCreateTable()) {
			createTable(
					tableName,
					options.isServerSideLibraryEnabled(),
					options.isEnableBlockCache());
		}

		try {
			return new mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloWriter(
					createBatchWriter(tableName),
					this,
					tableName);
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Table does not exist",
					e);
		}
		return null;
	}

	public BatchWriter createBatchWriter(
			final String tableName )
			throws TableNotFoundException {
		final String qName = getQualifiedTableName(tableName);
		final BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(byteBufferSize);
		config.setMaxLatency(
				timeoutMillis,
				TimeUnit.MILLISECONDS);
		config.setMaxWriteThreads(numThreads);
		return connector.createBatchWriter(
				qName,
				config);

	}

	private boolean iteratorsAttached = false;

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		if (options.isCreateTable()) {
			// this checks for existence prior to create
			createTable(
					AbstractGeoWavePersistence.METADATA_TABLE,
					false,
					options.isEnableBlockCache());
		}
		if (MetadataType.STATS.equals(metadataType) && options.isServerSideLibraryEnabled()) {
			synchronized (this) {
				if (!iteratorsAttached) {
					iteratorsAttached = true;

					final BasicOptionProvider optionProvider = new BasicOptionProvider(
							new HashMap<>());
					ServerOpHelper.addServerSideMerging(
							this,
							DataStatisticsStoreImpl.STATISTICS_COMBINER_NAME,
							DataStatisticsStoreImpl.STATS_COMBINER_PRIORITY,
							MergingCombiner.class.getName(),
							MergingVisibilityCombiner.class.getName(),
							optionProvider,
							AbstractGeoWavePersistence.METADATA_TABLE);
				}
			}
		}
		try {
			return new AccumuloMetadataWriter(
					createBatchWriter(AbstractGeoWavePersistence.METADATA_TABLE),
					metadataType);
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to create metadata writer",
					e);
		}
		return null;
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new AccumuloMetadataReader(
				this,
				options,
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new AccumuloMetadataDeleter(
				this,
				metadataType);
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		return compactTable(index.getId().getString());
	}

	public boolean compactTable(
			final String unqualifiedTableName ) {
		final String tableName = getQualifiedTableName(unqualifiedTableName);
		try {
			LOGGER.info("Compacting table '" + tableName + "'");
			connector.tableOperations().compact(
					tableName,
					null,
					null,
					true,
					true);
			LOGGER.info("Successfully compacted table '" + tableName + "'");
		}
		catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
			LOGGER.error(
					"Unable to merge data by compacting table '" + tableName + "'",
					e);
			return false;
		}
		return true;
	}

	public void enableVersioningIterator(
			final String tableName,
			final boolean enable )
			throws AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {
		synchronized (this) {
			final String qName = getQualifiedTableName(tableName);

			if (enable) {
				connector.tableOperations().attachIterator(
						qName,
						new IteratorSetting(
								20,
								"vers",
								VersioningIterator.class.getName()),
						EnumSet.allOf(IteratorScope.class));
			}
			else {
				connector.tableOperations().removeIterator(
						qName,
						"vers",
						EnumSet.allOf(IteratorScope.class));
			}
		}
	}

	public void setMaxVersions(
			final String tableName,
			final int maxVersions )
			throws AccumuloException,
			TableNotFoundException,
			AccumuloSecurityException {
		for (final IteratorScope iterScope : IteratorScope.values()) {
			connector.tableOperations().setProperty(
					getQualifiedTableName(tableName),
					Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers.opt.maxVersions",
					Integer.toString(maxVersions));
		}
	}

	@Override
	public Map<String, ImmutableSet<ServerOpScope>> listServerOps(
			final String index ) {
		try {
			return Maps.transformValues(
					connector.tableOperations().listIterators(
							getQualifiedTableName(index)),
					new Function<EnumSet<IteratorScope>, ImmutableSet<ServerOpScope>>() {

						@Override
						public ImmutableSet<ServerOpScope> apply(
								final EnumSet<IteratorScope> input ) {
							return Sets.immutableEnumSet(Iterables.transform(
									input,
									new Function<IteratorScope, ServerOpScope>() {

										@Override
										public ServerOpScope apply(
												final IteratorScope input ) {
											return fromAccumulo(input);
										}
									}));
						}
					});
		}
		catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			LOGGER.error(
					"Unable to list iterators for table '" + index + "'",
					e);
		}
		return null;
	}

	private static IteratorScope toAccumulo(
			final ServerOpScope scope ) {
		switch (scope) {
			case MAJOR_COMPACTION:
				return IteratorScope.majc;
			case MINOR_COMPACTION:
				return IteratorScope.minc;
			case SCAN:
				return IteratorScope.scan;
		}
		return null;
	}

	private static ServerOpScope fromAccumulo(
			final IteratorScope scope ) {
		switch (scope) {
			case majc:
				return ServerOpScope.MAJOR_COMPACTION;
			case minc:
				return ServerOpScope.MINOR_COMPACTION;
			case scan:
				return ServerOpScope.SCAN;
		}
		return null;
	}

	private static EnumSet<IteratorScope> toEnumSet(
			final ImmutableSet<ServerOpScope> scopes ) {
		final Collection<IteratorScope> c = Collections2.transform(
				scopes,
				new Function<ServerOpScope, IteratorScope>() {

					@Override
					public IteratorScope apply(
							@Nonnull
							final ServerOpScope scope ) {
						return toAccumulo(scope);
					}
				});
		EnumSet<IteratorScope> itSet;
		if (!c.isEmpty()) {
			final Iterator<IteratorScope> it = c.iterator();
			final IteratorScope first = it.next();
			final IteratorScope[] rest = new IteratorScope[c.size() - 1];
			int i = 0;
			while (it.hasNext()) {
				rest[i++] = it.next();
			}
			itSet = EnumSet.of(
					first,
					rest);
		}
		else {
			itSet = EnumSet.noneOf(IteratorScope.class);
		}
		return itSet;
	}

	@Override
	public Map<String, String> getServerOpOptions(
			final String index,
			final String serverOpName,
			final ServerOpScope scope ) {
		try {
			final IteratorSetting setting = connector.tableOperations().getIteratorSetting(
					getQualifiedTableName(index),
					serverOpName,
					toAccumulo(scope));
			if (setting != null) {
				return setting.getOptions();
			}
		}
		catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			LOGGER.error(
					"Unable to get iterator options for table '" + index + "'",
					e);
		}
		return Collections.emptyMap();
	}

	@Override
	public void removeServerOp(
			final String index,
			final String serverOpName,
			final ImmutableSet<ServerOpScope> scopes ) {

		try {
			connector.tableOperations().removeIterator(
					getQualifiedTableName(index),
					serverOpName,
					toEnumSet(scopes));
		}
		catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			LOGGER.error(
					"Unable to remove iterator",
					e);
		}
	}

	@Override
	public void addServerOp(
			final String index,
			final int priority,
			final String name,
			final String operationClass,
			final Map<String, String> properties,
			final ImmutableSet<ServerOpScope> configuredScopes ) {
		try {
			connector.tableOperations().attachIterator(
					getQualifiedTableName(index),
					new IteratorSetting(
							priority,
							name,
							operationClass,
							properties),
					toEnumSet(configuredScopes));
		}
		catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
			LOGGER.error(
					"Unable to attach iterator",
					e);
		}
	}

	@Override
	public void updateServerOp(
			final String index,
			final int priority,
			final String name,
			final String operationClass,
			final Map<String, String> properties,
			final ImmutableSet<ServerOpScope> currentScopes,
			final ImmutableSet<ServerOpScope> newScopes ) {
		removeServerOp(
				index,
				name,
				currentScopes);
		addServerOp(
				index,
				priority,
				name,
				operationClass,
				properties,
				newScopes);
	}

	public boolean isRowMergingEnabled(
			final short internalAdapterId,
			final String indexId ) {
		return DataAdapterAndIndexCache.getInstance(
				RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID,
				tableNamespace,
				AccumuloStoreFactoryFamily.TYPE).add(
				internalAdapterId,
				indexId);
	}

	@Override
	public boolean metadataExists(
			final MetadataType type )
			throws IOException {
		final String qName = getQualifiedTableName(AbstractGeoWavePersistence.METADATA_TABLE);
		return connector.tableOperations().exists(
				qName);
	}

	@Override
	public String getVersion() {
		// this just creates it if it doesn't exist
		createTable(
				AbstractGeoWavePersistence.METADATA_TABLE,
				true,
				true);
		try {
			final Scanner scanner = createScanner(AbstractGeoWavePersistence.METADATA_TABLE);
			scanner.addScanIterator(new IteratorSetting(
					25,
					VersionIterator.class));
			return StringUtils.stringFromBinary(scanner.iterator().next().getValue().get());
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Unable to get GeoWave version from Accumulo",
					e);
		}
		return null;
	}

}
