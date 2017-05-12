package mil.nga.giat.geowave.datastore.accumulo;

import java.util.ArrayList;
import java.util.Arrays;
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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;

/**
 * This class holds all parameters necessary for establishing Accumulo
 * connections and provides basic factory methods for creating a batch scanner
 * and a batch writer
 */
public class BasicAccumuloOperations implements
		AccumuloOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BasicAccumuloOperations.class);
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
	private final Map<String, Set<String>> insuredAuthorizationCache;

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
	public BasicAccumuloOperations(
			final String zookeeperUrl,
			final String instanceName,
			final String userName,
			final String password,
			final String tableNamespace )
			throws AccumuloException,
			AccumuloSecurityException {
		this(
				null,
				tableNamespace);

		this.password = password;

		connector = ConnectorPool.getInstance().getConnector(
				zookeeperUrl,
				instanceName,
				userName,
				this.password);
	}

	/**
	 * This constructor uses reasonable defaults and only requires an Accumulo
	 * connector
	 *
	 * @param connector
	 *            The connector to use for all operations
	 */
	public BasicAccumuloOperations(
			final Connector connector ) {
		this(
				connector,
				DEFAULT_TABLE_NAMESPACE);
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
	public BasicAccumuloOperations(
			final Connector connector,
			final String tableNamespace ) {
		this(
				DEFAULT_NUM_THREADS,
				DEFAULT_TIMEOUT_MILLIS,
				DEFAULT_BYTE_BUFFER_SIZE,
				DEFAULT_AUTHORIZATION,
				tableNamespace,
				connector);
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
	public BasicAccumuloOperations(
			final int numThreads,
			final long timeoutMillis,
			final long byteBufferSize,
			final String authorization,
			final String tableNamespace,
			final Connector connector ) {
		this.numThreads = numThreads;
		this.timeoutMillis = timeoutMillis;
		this.byteBufferSize = byteBufferSize;
		this.authorization = authorization;
		this.tableNamespace = tableNamespace;
		this.connector = connector;
		locGrpCache = new HashMap<String, Long>();
		insuredAuthorizationCache = new HashMap<String, Set<String>>();
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

	@Override
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

	@Override
	public Writer createWriter(
			final String tableName )
			throws TableNotFoundException {
		return createWriter(
				tableName,
				true);
	}

	@Override
	public Writer createWriter(
			final String tableName,
			final boolean createTable )
			throws TableNotFoundException {
		return createWriter(
				tableName,
				createTable,
				true,
				true,
				null);
	}

	@Override
	public Writer createWriter(
			final String tableName,
			final boolean createTable,
			final boolean enableVersioning,
			final boolean enableBlockCache,
			final Set<ByteArrayId> splits )
			throws TableNotFoundException {
		final String qName = getQualifiedTableName(tableName);
		if (createTable) {
			createTable(
					tableName,
					enableVersioning,
					enableBlockCache,
					splits);
		}
		final BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(byteBufferSize);
		config.setMaxLatency(
				timeoutMillis,
				TimeUnit.MILLISECONDS);
		config.setMaxWriteThreads(numThreads);
		return new mil.nga.giat.geowave.datastore.accumulo.BatchWriterWrapper(
				connector.createBatchWriter(
						qName,
						config));
	}

	@Override
	public void createTable(
			final String tableName,
			final boolean enableVersioning,
			final boolean enableBlockCache,
			final Set<ByteArrayId> splits ) {
		final String qName = getQualifiedTableName(tableName);

		if (!connector.tableOperations().exists(
				qName)) {
			try {
				connector.tableOperations().create(
						qName,
						enableVersioning);
				if (enableBlockCache) {
					connector.tableOperations().setProperty(
							qName,
							Property.TABLE_BLOCKCACHE_ENABLED.getKey(),
							"true");
				}
				if ((splits != null) && !splits.isEmpty()) {
					final SortedSet<Text> partitionKeys = new TreeSet<Text>();
					for (final ByteArrayId split : splits) {
						partitionKeys.add(new Text(
								split.getBytes()));
					}
					connector.tableOperations().addSplits(
							qName,
							partitionKeys);
				}
			}
			catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException e) {
				LOGGER.warn(
						"Unable to create table '" + qName + "'",
						e);
			}
		}
	}

	@Override
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

	@Override
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

	@Override
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
	}

	@Override
	public boolean delete(
			final String tableName,
			final ByteArrayId rowId,
			final String columnFamily,
			final String columnQualifier,
			final String... additionalAuthorizations ) {
		return this.delete(
				tableName,
				Arrays.asList(rowId),
				columnFamily,
				columnQualifier,
				additionalAuthorizations);
	}

	@Override
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

	@Override
	public boolean delete(
			final String tableName,
			final List<ByteArrayId> rowIds,
			final String columnFamily,
			final String columnQualifier,
			final String... authorizations ) {

		boolean success = true;
		BatchDeleter deleter = null;
		try {
			deleter = createBatchDeleter(
					tableName,
					authorizations);
			if ((columnFamily != null) && !columnFamily.isEmpty()) {
				if ((columnQualifier != null) && !columnQualifier.isEmpty()) {
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

	@Override
	public boolean tableExists(
			final String tableName ) {
		final String qName = getQualifiedTableName(tableName);
		return connector.tableOperations().exists(
				qName);
	}

	@Override
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

	@Override
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

	@Override
	public Scanner createScanner(
			final String tableName,
			final String... additionalAuthorizations )
			throws TableNotFoundException {
		return connector.createScanner(
				getQualifiedTableName(tableName),
				new Authorizations(
						getAuthorizations(additionalAuthorizations)));
	}

	@Override
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
	public void insureAuthorization(
			final String clientUser,
			final String... authorizations )
			throws AccumuloException,
			AccumuloSecurityException {
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
			Authorizations auths = connector.securityOperations().getUserAuthorizations(
					user);
			final List<byte[]> newSet = new ArrayList<byte[]>();
			for (final String auth : uninsuredAuths) {
				if (!auths.contains(auth)) {
					newSet.add(auth.getBytes(StringUtils.GEOWAVE_CHAR_SET));
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
	}

	@Override
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

	@Override
	public boolean attachIterators(
			final String tableName,
			final boolean createTable,
			final boolean enableVersioning,
			final boolean enableBlockCache,
			final Set<ByteArrayId> splits,
			final IteratorConfig... iterators )
			throws TableNotFoundException {
		final String qName = getQualifiedTableName(tableName);
		if (createTable && !connector.tableOperations().exists(
				qName)) {
			createTable(
					tableName,
					enableVersioning,
					enableBlockCache,
					splits);
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
		return false;
	}

	public static BasicAccumuloOperations createOperations(
			final AccumuloRequiredOptions options )
			throws AccumuloException,
			AccumuloSecurityException {
		return new BasicAccumuloOperations(
				options.getZookeeper(),
				options.getInstance(),
				options.getUser(),
				options.getPassword(),
				options.getGeowaveNamespace());
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

	@Override
	public String getGeoWaveNamespace() {
		return tableNamespace;
	}

	@Override
	public String getUsername() {
		return connector.whoami();
	}

	@Override
	public String getPassword() {
		return password;
	}

	@Override
	public Instance getInstance() {
		return connector.getInstance();
	}

	@Override
	public void addSplits(
			final String tableName,
			final boolean createTable,
			final Set<ByteArrayId> splits )
			throws TableNotFoundException,
			AccumuloException,
			AccumuloSecurityException {
		final String qName = getQualifiedTableName(tableName);
		if (createTable && !connector.tableOperations().exists(
				qName)) {
			try {
				connector.tableOperations().create(
						qName,
						true);
			}
			catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
				LOGGER.warn(
						"Unable to create table '" + qName + "'",
						e);
			}
		}
		final SortedSet<Text> partitionKeys = new TreeSet<Text>();
		for (final ByteArrayId split : splits) {
			partitionKeys.add(new Text(
					split.getBytes()));
		}
		connector.tableOperations().addSplits(
				qName,
				partitionKeys);
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {

		final String tableName = getQualifiedTableName(index.getId().getString());
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
}
