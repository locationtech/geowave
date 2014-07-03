package mil.nga.giat.geowave.accumulo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import mil.nga.giat.geowave.index.StringUtils;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * This class holds all parameters necessary for establishing Accumulo
 * connections and provides basic factory methods for creating a batch scanner
 * and a batch writer
 */
public class BasicAccumuloOperations implements
		AccumuloOperations
{
	private final static Logger logger = Logger.getLogger(BasicAccumuloOperations.class);
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
	private Connector connector;

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

		final Instance inst = new ZooKeeperInstance(
				instanceName,
				zookeeperUrl);
		connector = inst.getConnector(
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

	public String getAuthorization() {
		return authorization;
	}

	public Connector getConnector() {
		return connector;
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
		final String qName = getQualifiedTableName(tableName);
		if (createTable && !connector.tableOperations().exists(
				qName)) {
			try {
				connector.tableOperations().create(
						qName);
			}
			catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
				logger.warn(
						"Unable to create table '" + qName + "'",
						e);
			}
		}
		return new mil.nga.giat.geowave.accumulo.BatchWriter(
				connector.createBatchWriter(
						qName,
						byteBufferSize,
						timeoutMillis,
						numThreads));
	}

	@Override
	public BatchScanner createBatchScanner(
			final String tableName )
			throws TableNotFoundException {
		return connector.createBatchScanner(
				getQualifiedTableName(tableName),
				authorization == null ? new Authorizations() : new Authorizations(
						authorization),
				numThreads);
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
			logger.warn(
					"Unable to delete table, table not found '" + qName + "'",
					e);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			logger.warn(
					"Unable to delete table '" + qName + "'",
					e);
		}
		return false;
	}

	private String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return AccumuloUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

	@Override
	public boolean deleteAll() {
		SortedSet<String> tableNames = connector.tableOperations().list();

		if ((tableNamespace != null) && !tableNamespace.isEmpty()) {
			tableNames = tableNames.subSet(
					tableNamespace,
					tableNamespace + '\uffff');
		}
		boolean success = !tableNames.isEmpty();
		for (final String tableName : tableNames) {
			try {
				connector.tableOperations().delete(
						tableName);
			}
			catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
				logger.warn(
						"Unable to delete table '" + tableName + "'",
						e);
				success = false;
			}
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
		return connector.tableOperations().exists(
				qName) && connector.tableOperations().getLocalityGroups(
				qName).keySet().contains(
				StringUtils.stringFromBinary(localityGroup));
	}

	@Override
	public void addLocalityGroup(
			final String tableName,
			final byte[] localityGroup )
			throws AccumuloException,
			TableNotFoundException,
			AccumuloSecurityException {
		final String qName = getQualifiedTableName(tableName);

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
		}
	}

	@Override
	public Scanner createScanner(
			final String tableName )
			throws TableNotFoundException {
		return connector.createScanner(
				getQualifiedTableName(tableName),
				authorization == null ? new Authorizations() : new Authorizations(
						authorization));
	}
}
