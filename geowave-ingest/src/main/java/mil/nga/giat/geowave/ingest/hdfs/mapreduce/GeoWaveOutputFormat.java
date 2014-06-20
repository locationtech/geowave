package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.MemoryIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GeoWaveOutputFormat extends
		OutputFormat<ByteArrayId, Object>
{

	private static final Class<?> CLASS = GeoWaveOutputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(CLASS);

	@Override
	public RecordWriter<ByteArrayId, Object> getRecordWriter(
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		try {
			final AccumuloOperations accumuloOperations = getAccumuloOperations(context);
			final AdapterStore adapterStore = getDataAdapterStore(
					context,
					accumuloOperations);
			return new GeoWaveRecordWriter(
					context,
					accumuloOperations,
					getIndex(context),
					adapterStore);
		}
		catch (final Exception e) {
			throw new IOException(
					e);
		}
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
		if (getIndex(context) == null) {
			LOGGER.warn("GeoWave index is missing from job context");
			throw new IOException(
					"GeoWave index is missing from job context");
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
	protected class GeoWaveRecordWriter extends
			RecordWriter<ByteArrayId, Object>
	{
		private final IndexWriter indexWriter;
		private final AdapterStore adapterStore;

		protected GeoWaveRecordWriter(
				final TaskAttemptContext context,
				final AccumuloOperations accumuloOperations,
				final Index index,
				final AdapterStore adapterStore )
				throws AccumuloException,
				AccumuloSecurityException,
				IOException {
			final Level l = getLogLevel(context);
			if (l != null) {
				LOGGER.setLevel(getLogLevel(context));
			}
			final DataStore dataStore = new AccumuloDataStore(
					new MemoryIndexStore(
							new Index[] {
								index
							}),
					adapterStore,
					accumuloOperations);
			this.adapterStore = adapterStore;
			indexWriter = dataStore.createIndexWriter(index);
		}

		/**
		 * Push a mutation into a table. If table is null, the defaultTable will
		 * be used. If canCreateTable is set, the table will be created if it
		 * does not exist. The table name must only contain alphanumerics and
		 * underscore.
		 */
		@Override
		public void write(
				final ByteArrayId adapterId,
				final Object object )
				throws IOException {
			final DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
			if (adapter instanceof WritableDataAdapter) {
				indexWriter.write(
						(WritableDataAdapter) adapter,
						object);
			}
			else {
				LOGGER.warn("Adapter '" + StringUtils.stringFromBinary(adapterId.getBytes()) + "' is not writable");
			}
		}

		@Override
		public void close(
				final TaskAttemptContext attempt )
				throws IOException,
				InterruptedException {
			indexWriter.close();
		}
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
		GeoWaveConfiguratorBase.setZookeeperUrl(
				CLASS,
				job,
				zooKeepers);
		GeoWaveConfiguratorBase.setInstanceName(
				CLASS,
				job,
				instanceName);
		GeoWaveConfiguratorBase.setUserName(
				CLASS,
				job,
				userName);
		GeoWaveConfiguratorBase.setPassword(
				CLASS,
				job,
				password);
		GeoWaveConfiguratorBase.setTableNamespace(
				CLASS,
				job,
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
			final Job job,
			final Level level ) {
		ConfiguratorBase.setLogLevel(
				CLASS,
				job.getConfiguration(),
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

	public static AdapterStore getDataAdapterStore(
			final JobContext context,
			final AccumuloOperations accumuloOperations ) {
		return new JobContextAdapterStore(
				context,
				accumuloOperations);
	}

	protected static Index getIndex(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getIndex(
				CLASS,
				context);
	}

	protected static DataAdapter<?> getDataAdapter(
			final JobContext context,
			final ByteArrayId adapterId ) {
		return GeoWaveConfiguratorBase.getDataAdapter(
				CLASS,
				context,
				adapterId);
	}

	public static void addDataAdapter(
			final Job job,
			final DataAdapter<?> adapter ) {
		GeoWaveConfiguratorBase.addDataAdapter(
				CLASS,
				job,
				adapter);
	}

	public static void setIndex(
			final Job job,
			final Index index ) {
		GeoWaveConfiguratorBase.setIndex(
				CLASS,
				job,
				index);
	}
}
