package mil.nga.giat.geowave.datastore.accumulo.mapreduce.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This output format is the preferred mechanism for writing data to GeoWave
 * within a map-reduce job.
 */
public class GeoWaveOutputFormat extends
		OutputFormat<GeoWaveOutputKey, Object>
{

	private static final Class<?> CLASS = GeoWaveOutputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);

	@Override
	public RecordWriter<GeoWaveOutputKey, Object> getRecordWriter(
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		try {
			// TODO expose GeoWave's AccumuloOptions
			final AccumuloOperations accumuloOperations = getAccumuloOperations(context);
			final AdapterStore accumuloAdapterStore = new AccumuloAdapterStore(
					accumuloOperations);
			final DataAdapter<?>[] adapters = JobContextAdapterStore.getDataAdapters(context);
			for (final DataAdapter<?> a : adapters) {
				if (!accumuloAdapterStore.adapterExists(a.getAdapterId())) {
					accumuloAdapterStore.addAdapter(a);
				}
			}
			final IndexStore accumuloIndexStore = new AccumuloIndexStore(
					accumuloOperations);
			final Index[] indices = JobContextIndexStore.getIndices(context);
			for (final Index i : indices) {
				if (!accumuloIndexStore.indexExists(i.getId())) {
					accumuloIndexStore.addIndex(i);
				}
			}
			final AdapterStore jobContextAdapterStore = getDataAdapterStore(
					context,
					accumuloOperations);
			final IndexStore jobContextIndexStore = getIndexStore(
					context,
					accumuloOperations);
			final DataStatisticsStore statisticsStore = new AccumuloDataStatisticsStore(
					accumuloOperations);
			return new GeoWaveRecordWriter(
					context,
					accumuloOperations,
					jobContextIndexStore,
					jobContextAdapterStore,
					statisticsStore);
		}
		catch (final Exception e) {
			throw new IOException(
					e);
		}
	}

	public static void addIndex(
			final Configuration config,
			final Index index ) {
		JobContextIndexStore.addIndex(
				config,
				index);
	}

	public static void addDataAdapter(
			final Configuration config,
			final DataAdapter<?> adapter ) {
		JobContextAdapterStore.addDataAdapter(
				config,
				adapter);
	}

	protected static IndexStore getIndexStore(
			final JobContext context,
			final AccumuloOperations accumuloOperations ) {
		return new JobContextIndexStore(
				context,
				accumuloOperations);
	}

	protected static AdapterStore getDataAdapterStore(
			final JobContext context,
			final AccumuloOperations accumuloOperations ) {
		return new JobContextAdapterStore(
				context,
				accumuloOperations);
	}

	@Override
	public void checkOutputSpecs(
			final JobContext context )
			throws IOException,
			InterruptedException {
		// the two required elements are the AccumuloOperations info and the
		// Index
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

	@Override
	public OutputCommitter getOutputCommitter(
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		return new NullOutputFormat<ByteArrayId, Object>().getOutputCommitter(context);
	}

	/**
	 * A base class to be used to create {@link RecordWriter} instances that
	 * write to Accumulo.
	 */
	protected static class GeoWaveRecordWriter extends
			RecordWriter<GeoWaveOutputKey, Object>
	{
		private final Map<ByteArrayId, IndexWriter> indexWriterCache = new HashMap<ByteArrayId, IndexWriter>();
		private final AdapterStore adapterStore;
		private final IndexStore indexStore;
		private final DataStore dataStore;

		protected GeoWaveRecordWriter(
				final TaskAttemptContext context,
				final AccumuloOperations accumuloOperations,
				final IndexStore indexStore,
				final AdapterStore adapterStore,
				final DataStatisticsStore statisticsStore )
				throws AccumuloException,
				AccumuloSecurityException,
				IOException {
			final Level l = getLogLevel(context);
			if (l != null) {
				LOGGER.setLevel(getLogLevel(context));
			}
			dataStore = new AccumuloDataStore(
					indexStore,
					adapterStore,
					statisticsStore,
					accumuloOperations);
			this.adapterStore = adapterStore;
			this.indexStore = indexStore;
		}

		/**
		 * Push a mutation into a table. If table is null, the defaultTable will
		 * be used. If canCreateTable is set, the table will be created if it
		 * does not exist. The table name must only contain alphanumerics and
		 * underscore.
		 */
		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public void write(
				final GeoWaveOutputKey ingestKey,
				final Object object )
				throws IOException {
			final DataAdapter<?> adapter = adapterStore.getAdapter(ingestKey.getAdapterId());
			if (adapter instanceof WritableDataAdapter) {
				final IndexWriter indexWriter = getIndexWriter(ingestKey.getIndexId());
				if (indexWriter != null) {
					indexWriter.write(
							(WritableDataAdapter) adapter,
							object);
				}
				else {
					LOGGER.warn("Cannot write to index '" + StringUtils.stringFromBinary(ingestKey.getAdapterId().getBytes()) + "'");
				}
			}
			else {
				LOGGER.warn("Adapter '" + StringUtils.stringFromBinary(ingestKey.getAdapterId().getBytes()) + "' is not writable");
			}
		}

		private synchronized IndexWriter getIndexWriter(
				final ByteArrayId indexId ) {
			if (!indexWriterCache.containsKey(indexId)) {
				final Index index = indexStore.getIndex(indexId);
				IndexWriter writer = null;
				if (index != null) {
					writer = dataStore.createIndexWriter(index);
				}
				else {
					LOGGER.warn("Index '" + StringUtils.stringFromBinary(indexId.getBytes()) + "' does not exist");
				}
				indexWriterCache.put(
						indexId,
						writer);
				return writer;
			}
			return indexWriterCache.get(indexId);
		}

		@Override
		public synchronized void close(
				final TaskAttemptContext attempt )
				throws IOException,
				InterruptedException {
			for (final IndexWriter indexWriter : indexWriterCache.values()) {
				indexWriter.close();
			}
		}
	}

	/**
	 * Configures a {@link AccumuloOperations} for this job.
	 * 
	 * @param config
	 *            hadoop configuration
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
		GeoWaveConfiguratorBase.setZookeeperUrl(
				CLASS,
				config,
				zooKeepers);
		GeoWaveConfiguratorBase.setInstanceName(
				CLASS,
				config,
				instanceName);
		GeoWaveConfiguratorBase.setUserName(
				CLASS,
				config,
				userName);
		GeoWaveConfiguratorBase.setPassword(
				CLASS,
				config,
				password);
		GeoWaveConfiguratorBase.setTableNamespace(
				CLASS,
				config,
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
	 * Sets the log level for this job.
	 * 
	 * @param job
	 *            the Hadoop job instance to be configured
	 * @param level
	 *            the logging level
	 * @since 1.5.0
	 */
	public static void setLogLevel(
			Configuration config,
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

	// TODO add enabling/disabling features such as automatic table, data
	// adapter, and index creation; for now that is not an option exposed in
	// GeoWaveDataStore so it will automatically create anything that doesn't
	// exist
	/**
	 * Sets the directive to create new tables, as necessary. Table names can
	 * only be alpha-numeric and underscores.
	 * 
	 * <p>
	 * By default, this feature is <b>disabled</b>.
	 * 
	 * @param job
	 *            the Hadoop job instance to be configured
	 * @param enableFeature
	 *            the feature is enabled if true, disabled otherwise
	 */
	// public static void setCreateTables(
	// final Job job,
	// final boolean enableFeature ) {
	// GeoWaveOutputConfigurator.setCreateTables(
	// CLASS,
	// job,
	// enableFeature);
	// }

	/**
	 * Determines whether tables are permitted to be created as needed.
	 * 
	 * @param context
	 *            the Hadoop context for the configured job
	 * @return true if the feature is disabled, false otherwise
	 * @see #setCreateTables(Job, boolean)
	 */
	// protected static Boolean canCreateTables(
	// final JobContext context ) {
	// return GeoWaveOutputConfigurator.canCreateTables(
	// CLASS,
	// context);
	// }

	public static AccumuloOperations getAccumuloOperations(
			final JobContext context )
			throws AccumuloException,
			AccumuloSecurityException {
		return GeoWaveConfiguratorBase.getAccumuloOperations(
				CLASS,
				context);
	}
}
