package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

/**
 * This class forms the basis for GeoWave input and output format configuration.
 */
public class GeoWaveConfiguratorBase
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveConfiguratorBase.class);

	protected static enum GeoWaveMetaStore {
		INDEX,
		DATA_ADAPTER,
	}

	/**
	 * Configuration keys for AccumuloOperations configuration.
	 * 
	 */
	protected static enum AccumuloOperationsConfig {
		ZOOKEEPER_URL,
		INSTANCE_NAME,
		USER_NAME,
		PASSWORD,
		TABLE_NAMESPACE
	}

	// TODO use these options to restrict creation when set (currently, within
	// the GeoWavevDataStore if anything doesn't exist it is automatically
	// persisted)
	protected static enum GeneralConfig {
		CREATE_ADAPTERS,
		CREATE_INDEX,
		CREATE_TABLES
	}

	/**
	 * Provides a configuration key for a given feature enum, prefixed by the
	 * implementingClass, and suffixed by a custom String
	 * 
	 * @param implementingClass
	 *            the class whose name will be used as a prefix for the property
	 *            configuration key
	 * @param e
	 *            the enum used to provide the unique part of the configuration
	 *            key
	 * 
	 * @param suffix
	 *            the custom suffix to be used in the configuration key
	 * @return the configuration key
	 */
	public static String enumToConfKey(
			final Class<?> implementingClass,
			final Enum<?> e,
			final String suffix ) {
		return enumToConfKey(
				implementingClass,
				e) + "-" + suffix;
	}

	/**
	 * Provides a configuration key for a given feature enum, prefixed by the
	 * implementingClass
	 * 
	 * @param implementingClass
	 *            the class whose name will be used as a prefix for the property
	 *            configuration key
	 * @param e
	 *            the enum used to provide the unique part of the configuration
	 *            key
	 * @return the configuration key
	 */
	public static String enumToConfKey(
			final Class<?> implementingClass,
			final Enum<?> e ) {
		String s = implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "." + org.apache.hadoop.util.StringUtils.camelize(e.name().toLowerCase());
		return s;
	}

	public static final <T> T getInstance(
			final Class<?> implementingClass,
			final Enum<?> e,
			final JobContext context,
			final Class<T> interfaceClass )
			throws InstantiationException,
			IllegalAccessException {
		return (T) getConfiguration(
				context).getClass(
				enumToConfKey(
						implementingClass,
						e),
				interfaceClass).newInstance();
	}

	public static final <T> T getInstance(
			final Class<?> implementingClass,
			final Enum<?> e,
			final JobContext context,
			final Class<T> interfaceClass,
			final Class<? extends T> defaultClass )
			throws InstantiationException,
			IllegalAccessException {
		return (T) getConfiguration(
				context).getClass(
				enumToConfKey(
						implementingClass,
						e),
				defaultClass,
				interfaceClass).newInstance();
	}

	public static void setZookeeperUrl(
			final Class<?> implementingClass,
			final Configuration config,
			final String zookeeperUrl ) {
		if (zookeeperUrl != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							AccumuloOperationsConfig.ZOOKEEPER_URL),
					zookeeperUrl);
		}

	}

	public static AccumuloOperations getAccumuloOperations(
			final Class<?> implementingClass,
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {
		final String zookeeperURL = getZookeeperUrl(
				implementingClass,
				context);
		if (zookeeperURL != null && zookeeperURL.length() > 0)
			return new BasicAccumuloOperations(
					zookeeperURL,
					getInstanceName(
							implementingClass,
							context),
					getUserName(
							implementingClass,
							context),
					getPassword(
							implementingClass,
							context),
					getTableNamespace(
							implementingClass,
							context));
		else {
			return null;
		}
	}

	public static String getZookeeperUrl(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getZookeeperUrlInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setInstanceName(
			final Class<?> implementingClass,
			final Configuration config,
			final String instanceName ) {
		if (instanceName != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							AccumuloOperationsConfig.INSTANCE_NAME),
					instanceName);
		}

	}

	public static String getInstanceName(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getInstanceNameInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setUserName(
			final Class<?> implementingClass,
			final Configuration config,
			final String userName ) {
		if (userName != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							AccumuloOperationsConfig.USER_NAME),
					userName);
		}

	}

	public static String getUserName(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getUserNameInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setPassword(
			final Class<?> implementingClass,
			final Configuration config,
			final String password ) {
		if (password != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							AccumuloOperationsConfig.PASSWORD),
					password);
		}
	}

	public static String getPassword(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getPasswordInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setTableNamespace(
			final Class<?> implementingClass,
			final Configuration config,
			final String tableNamespace ) {
		if (tableNamespace != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							AccumuloOperationsConfig.TABLE_NAMESPACE),
					tableNamespace);
		}
	}

	public static String getTableNamespace(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getTableNamespaceInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void addIndex(
			final Class<?> implementingClass,
			final Configuration config,
			final Index index ) {
		if (index != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							GeoWaveMetaStore.INDEX,
							index.getId().getString()),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index)));
		}
	}

	public static Index getIndex(
			final Class<?> implementingClass,
			final JobContext context,
			final ByteArrayId indexId ) {
		return getIndexInternal(
				implementingClass,
				getConfiguration(context),
				indexId);
	}

	public static void addDataAdapter(
			final Class<?> implementingClass,
			final Configuration conf,
			final DataAdapter<?> adapter ) {
		if (adapter != null) {
			conf.set(
					enumToConfKey(
							implementingClass,
							GeoWaveMetaStore.DATA_ADAPTER,
							adapter.getAdapterId().getString()),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(adapter)));
		}
	}

	public static DataAdapter<?> getDataAdapter(
			final Class<?> implementingClass,
			final JobContext context,
			final ByteArrayId adapterId ) {
		return getDataAdapterInternal(
				implementingClass,
				getConfiguration(context),
				adapterId);
	}

	private static DataAdapter<?> getDataAdapterInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final ByteArrayId adapterId ) {
		final String input = configuration.get(enumToConfKey(
				implementingClass,
				GeoWaveMetaStore.DATA_ADAPTER,
				adapterId.getString()));
		if (input != null) {
			final byte[] dataAdapterBytes = ByteArrayUtils.byteArrayFromString(input);
			return PersistenceUtils.fromBinary(
					dataAdapterBytes,
					DataAdapter.class);
		}
		return null;
	}

	public static DataAdapter<?>[] getDataAdapters(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getDataAdaptersInternal(
				implementingClass,
				getConfiguration(context));
	}

	private static DataAdapter<?>[] getDataAdaptersInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final Map<String, String> input = configuration.getValByRegex(enumToConfKey(
				implementingClass,
				GeoWaveMetaStore.DATA_ADAPTER) + "*");
		if (input != null) {
			final List<DataAdapter<?>> adapters = new ArrayList<DataAdapter<?>>(
					input.size());
			for (final String dataAdapterStr : input.values()) {
				final byte[] dataAdapterBytes = ByteArrayUtils.byteArrayFromString(dataAdapterStr);
				adapters.add(PersistenceUtils.fromBinary(
						dataAdapterBytes,
						DataAdapter.class));
			}
			return adapters.toArray(new DataAdapter[adapters.size()]);
		}
		return new DataAdapter[] {};
	}

	private static Boolean canCreateAdaptersInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.getBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_ADAPTERS),
				true);
	}

	public static Boolean canCreateAdapters(
			final Class<?> implementingClass,
			final JobContext context ) {
		return canCreateAdaptersInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void setCreateAdapters(
			final Class<?> implementingClass,
			final Configuration config,
			final boolean enableFeature ) {
		config.setBoolean(
				enumToConfKey(
						implementingClass,
						GeneralConfig.CREATE_ADAPTERS),
				enableFeature);
	}

	private static Index getIndexInternal(
			final Class<?> implementingClass,
			final Configuration configuration,
			final ByteArrayId indexId ) {
		final String input = configuration.get(enumToConfKey(
				implementingClass,
				GeoWaveMetaStore.INDEX,
				indexId.getString()));
		if (input != null) {
			final byte[] indexBytes = ByteArrayUtils.byteArrayFromString(input);
			return PersistenceUtils.fromBinary(
					indexBytes,
					Index.class);
		}
		return null;
	}

	public static Index[] getIndices(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getIndicesInternal(
				implementingClass,
				getConfiguration(context));
	}

	private static Index[] getIndicesInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final Map<String, String> input = configuration.getValByRegex(enumToConfKey(
				implementingClass,
				GeoWaveMetaStore.INDEX) + "*");
		if (input != null) {
			final List<Index> indices = new ArrayList<Index>(
					input.size());
			for (final String indexStr : input.values()) {
				final byte[] indexBytes = ByteArrayUtils.byteArrayFromString(indexStr);
				indices.add(PersistenceUtils.fromBinary(
						indexBytes,
						Index.class));
			}
			return indices.toArray(new Index[indices.size()]);
		}
		return new Index[] {};
	}

	private static String getTableNamespaceInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						AccumuloOperationsConfig.TABLE_NAMESPACE),
				"");
	}

	private static String getInstanceNameInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						AccumuloOperationsConfig.INSTANCE_NAME),
				"");
	}

	private static String getZookeeperUrlInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						AccumuloOperationsConfig.ZOOKEEPER_URL),
				"");
	}

	private static String getUserNameInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						AccumuloOperationsConfig.USER_NAME),
				"");
	}

	private static String getPasswordInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						AccumuloOperationsConfig.PASSWORD),
				"");
	}

	// use reflection to pull the Configuration out of the JobContext for Hadoop
	// 1 and Hadoop 2 compatibility
	public static Configuration getConfiguration(
			final JobContext context ) {
		try {
			final Class<?> c = InputFormatBase.class.getClassLoader().loadClass(
					"org.apache.hadoop.mapreduce.JobContext");
			final Method m = c.getMethod("getConfiguration");
			final Object o = m.invoke(
					context,
					new Object[0]);
			return (Configuration) o;
		}
		catch (final Exception e) {
			throw new RuntimeException(
					e);
		}
	}

	public static void setRemoteInvocationParams(
			final String hdfsHostPort,
			final String jobTrackerOrResourceManagerHostPort,
			final Configuration conf ) {
		conf.set(
				"fs.defaultFS",
				hdfsHostPort);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		// if this property is used, it hadoop does not support yarn
		conf.set(
				"mapred.job.tracker",
				jobTrackerOrResourceManagerHostPort);
		// the following 3 properties will only be used if the hadoop version
		// does support yarn
		if ("local".equals(jobTrackerOrResourceManagerHostPort)) {
			conf.set(
					"mapreduce.framework.name",
					"local");
		}
		else {
			conf.set(
					"mapreduce.framework.name",
					"yarn");
		}
		conf.set(
				"yarn.resourcemanager.address",
				jobTrackerOrResourceManagerHostPort);
		// if remotely submitted with yarn, the job configuration xml will be
		// written to this staging directory, it is generally good practice to
		// ensure the staging directory is different for each user
		String user = System.getProperty("user.name");
		if ((user == null) || user.isEmpty()) {
			user = "default";
		}
		conf.set(
				"yarn.app.mapreduce.am.staging-dir",
				"/tmp/hadoop-" + user);
	}

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
			final Class<?> scope,
			final Configuration config,
			final String zooKeepers,
			final String instanceName,
			final String userName,
			final String password,
			final String geowaveTableNamespace ) {
		GeoWaveConfiguratorBase.setZookeeperUrl(
				scope,
				config,
				zooKeepers);
		GeoWaveConfiguratorBase.setInstanceName(
				scope,
				config,
				instanceName);
		GeoWaveConfiguratorBase.setUserName(
				scope,
				config,
				userName);
		GeoWaveConfiguratorBase.setPassword(
				scope,
				config,
				password);
		GeoWaveConfiguratorBase.setTableNamespace(
				scope,
				config,
				geowaveTableNamespace);
	}
}
