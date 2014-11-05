package mil.nga.giat.geowave.accumulo.mapreduce;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
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
	protected static String enumToConfKey(
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
	protected static String enumToConfKey(
			final Class<?> implementingClass,
			final Enum<?> e ) {
		return implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "." + org.apache.hadoop.util.StringUtils.camelize(e.name().toLowerCase());
	}

	public static void setZookeeperUrl(
			final Class<?> implementingClass,
			final Job job,
			final String zookeeperUrl ) {
		if (zookeeperUrl != null) {
			job.getConfiguration().set(
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
		return new BasicAccumuloOperations(
				getZookeeperUrl(
						implementingClass,
						context),
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
			final Job job,
			final String instanceName ) {
		if (instanceName != null) {
			job.getConfiguration().set(
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
			final Job job,
			final String userName ) {
		if (userName != null) {
			job.getConfiguration().set(
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
			final Job job,
			final String password ) {
		if (password != null) {
			job.getConfiguration().set(
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
			final Job job,
			final String tableNamespace ) {
		if (tableNamespace != null) {
			job.getConfiguration().set(
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
			final Job job,
			final Index index ) {
		if (index != null) {
			job.getConfiguration().set(
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
			final Job job,
			final DataAdapter<?> adapter ) {
		if (adapter != null) {
			job.getConfiguration().set(
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
			return adapters.toArray(new DataAdapter[] {});
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
			final Job job,
			final boolean enableFeature ) {
		job.getConfiguration().setBoolean(
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
			return indices.toArray(new Index[] {});
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
}
