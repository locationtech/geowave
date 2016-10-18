package mil.nga.giat.geowave.mapreduce;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This class forms the basis for GeoWave input and output format configuration.
 */
public class GeoWaveConfiguratorBase
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveConfiguratorBase.class);
	private static final String KEY_SEPARATOR = "-";

	public static enum GeoWaveMetaStore {
		INDEX,
		DATA_ADAPTER,
	}

	public static enum GeneralConfig {
		DATA_STORE_NAME,
		STORE_CONFIG_OPTION
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
				e) + KEY_SEPARATOR + suffix;
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
		final String s = implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "."
				+ org.apache.hadoop.util.StringUtils.camelize(e.name().toLowerCase());
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
		return getConfiguration(
				context).getClass(
				enumToConfKey(
						implementingClass,
						e),
				defaultClass,
				interfaceClass).newInstance();
	}

	public static DataStore getDataStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		return GeoWaveStoreFinder.createDataStore(getStoreConfigOptions(
					implementingClass,
				context));
	}

	public static DataStatisticsStore getDataStatisticsStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		return GeoWaveStoreFinder.createDataStatisticsStore(getStoreConfigOptions(
				implementingClass,
				context));
	}

	public static void setDataStoreName(
			final Class<?> implementingClass,
			final Configuration config,
			final String dataStoreName ) {
		if (dataStoreName != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							GeneralConfig.DATA_STORE_NAME),
					dataStoreName);
		}
	}

	public static void setStoreConfigOptions(
			final Class<?> implementingClass,
			final Configuration config,
			final Map<String, String> dataStoreConfigOptions ) {
		if ((dataStoreConfigOptions != null) && !dataStoreConfigOptions.isEmpty()) {
			for (final Entry<String, String> entry : dataStoreConfigOptions.entrySet()) {
				config.set(
						enumToConfKey(
								implementingClass,
								GeneralConfig.STORE_CONFIG_OPTION,
								entry.getKey()),
						entry.getValue());
			}
		}
	}

	public static Map<String, String> getStoreConfigOptions(
			final Class<?> implementingClass,
			final JobContext context ) {
		final String dataStoreName = getDataStoreName(
				implementingClass,
				context);
		final Map<String, String> configOptions = getConfigOptionsInternal(
				implementingClass,
				getConfiguration(context));
		if ((dataStoreName != null) && (!dataStoreName.isEmpty())) {
			configOptions.put(
					GeoWaveStoreFinder.STORE_HINT_OPTION.getName(),
					dataStoreName);
		}
		return configOptions;
	}

	public static String getDataStoreName(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getDataStoreNameInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static void addIndex(
			final Class<?> implementingClass,
			final Configuration config,
			final PrimaryIndex index ) {
		if (index != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							GeoWaveMetaStore.INDEX,
							index.getId().getString()),
					ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(index)));
		}
	}

	public static PrimaryIndex getIndex(
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

	private static Map<String, String> getConfigOptionsInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final String prefix = enumToConfKey(
				implementingClass,
				GeneralConfig.STORE_CONFIG_OPTION) + KEY_SEPARATOR;
		final Map<String, String> enumMap = configuration.getValByRegex(prefix + "*");
		final Map<String, String> retVal = new HashMap<String, String>();
		for (final Entry<String, String> entry : enumMap.entrySet()) {
			final String key = entry.getKey();
			retVal.put(
					key.substring(prefix.length()),
					entry.getValue());
		}
		return retVal;
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

	private static PrimaryIndex getIndexInternal(
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
					PrimaryIndex.class);
		}
		return null;
	}

	public static PrimaryIndex[] getIndices(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getIndicesInternal(
				implementingClass,
				getConfiguration(context));
	}

	public static IndexStore getJobContextIndexStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Map<String, String> configOptions = getStoreConfigOptions(
				implementingClass,
				context);
		return new JobContextIndexStore(
				context,
				GeoWaveStoreFinder.createIndexStore(configOptions));
	}

	public static AdapterStore getJobContextAdapterStore(
			final Class<?> implementingClass,
			final JobContext context ) {
		final Map<String, String> configOptions = getStoreConfigOptions(
				implementingClass,
				context);
		return new JobContextAdapterStore(
				context,
				GeoWaveStoreFinder.createAdapterStore(configOptions));
	}

	private static PrimaryIndex[] getIndicesInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		final Map<String, String> input = configuration.getValByRegex(enumToConfKey(
				implementingClass,
				GeoWaveMetaStore.INDEX) + "*");
		if (input != null) {
			final List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>(
					input.size());
			for (final String indexStr : input.values()) {
				final byte[] indexBytes = ByteArrayUtils.byteArrayFromString(indexStr);
				indices.add(PersistenceUtils.fromBinary(
						indexBytes,
						PrimaryIndex.class));
			}
			return indices.toArray(new PrimaryIndex[indices.size()]);
		}
		return new PrimaryIndex[] {};
	}

	private static String getDataStoreNameInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						GeneralConfig.DATA_STORE_NAME),
				"");
	}

	// use reflection to pull the Configuration out of the JobContext for Hadoop
	// 1 and Hadoop 2 compatibility
	public static Configuration getConfiguration(
			final JobContext context ) {
		try {
			final Class<?> c = GeoWaveConfiguratorBase.class.getClassLoader().loadClass(
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
}