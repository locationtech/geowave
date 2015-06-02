/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> GeoWaveConfiguratorBase </code>
 */
public class GeoWaveHBaseConfiguratorBase
{
	protected static final Logger LOGGER = Logger.getLogger(GeoWaveHBaseConfiguratorBase.class);

	protected static enum GeoWaveMetaStore {
		INDEX,
		DATA_ADAPTER,
	}

	/**
	 * Configuration keys for BasicHBaseOperations configuration.
	 * 
	 */
	protected static enum HBaseOperationsConfig {
		ZOOKEEPER_URL,
		TABLE_NAMESPACE
	}

	public static BasicHBaseOperations getOperations(
			final Class<?> implementingClass,
			final JobContext context )
			throws IOException {
		final String zookeeperURL = getZookeeperUrl(
				implementingClass,
				context);
		if (zookeeperURL != null && zookeeperURL.length() > 0)
			return new BasicHBaseOperations(
					zookeeperURL,
					getTableNamespace(
							implementingClass,
							context));
		else {
			return null;
		}
	}

	public static String getTableNamespace(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getTableNamespaceInternal(
				implementingClass,
				getConfiguration(context));
	}

	private static String getTableNamespaceInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						HBaseOperationsConfig.TABLE_NAMESPACE),
				"");
	}

	public static String getZookeeperUrl(
			final Class<?> implementingClass,
			final JobContext context ) {
		return getZookeeperUrlInternal(
				implementingClass,
				getConfiguration(context));
	}

	private static String getZookeeperUrlInternal(
			final Class<?> implementingClass,
			final Configuration configuration ) {
		return configuration.get(
				enumToConfKey(
						implementingClass,
						HBaseOperationsConfig.ZOOKEEPER_URL),
				"");
	}

	public static String enumToConfKey(
			final Class<?> implementingClass,
			final Enum<?> e ) {
		String s = implementingClass.getSimpleName() + "." + e.getDeclaringClass().getSimpleName() + "." + org.apache.hadoop.util.StringUtils.camelize(e.name().toLowerCase());
		return s;
	}

	public static Configuration getConfiguration(
			final JobContext context ) {
		return context.getConfiguration();
		/*
		 * try { final Class<?> c =
		 * InputFormatBase.class.getClassLoader().loadClass(
		 * "org.apache.hadoop.mapreduce.JobContext"); final Method m =
		 * c.getMethod("getConfiguration"); final Object o = m.invoke( context,
		 * new Object[0]); return (Configuration) o; } catch (final Exception e)
		 * { throw new RuntimeException( e); }
		 */
	}

	public static void setZookeeperUrl(
			final Class<?> implementingClass,
			final Configuration config,
			final String zookeeperUrl ) {
		if (zookeeperUrl != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							HBaseOperationsConfig.ZOOKEEPER_URL),
					zookeeperUrl);
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

	public static void setTableNamespace(
			final Class<?> implementingClass,
			final Configuration config,
			final String tableNamespace ) {
		if (tableNamespace != null) {
			config.set(
					enumToConfKey(
							implementingClass,
							HBaseOperationsConfig.TABLE_NAMESPACE),
					tableNamespace);
		}
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

	public static void setOperationsInfo(
			final Class<?> scope,
			final Configuration config,
			final String zooKeepers,
			final String geowaveTableNamespace ) {
		GeoWaveHBaseConfiguratorBase.setZookeeperUrl(
				scope,
				config,
				zooKeepers);
		GeoWaveHBaseConfiguratorBase.setTableNamespace(
				scope,
				config,
				geowaveTableNamespace);
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

	public static String enumToConfKey(
			final Class<?> implementingClass,
			final Enum<?> e,
			final String suffix ) {
		return enumToConfKey(
				implementingClass,
				e) + "-" + suffix;
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
}
