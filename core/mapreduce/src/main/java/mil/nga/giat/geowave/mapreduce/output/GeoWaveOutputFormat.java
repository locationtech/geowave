package mil.nga.giat.geowave.mapreduce.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
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
			final Map<String, Object> configOptions = ConfigUtils.valuesFromStrings(getStoreConfigOptions(context));
			final String namespace = getGeoWaveNamespace(context);
			final AdapterStore persistentAdapterStore = GeoWaveStoreFinder.createAdapterStore(
					configOptions,
					namespace);
			final DataAdapter<?>[] adapters = JobContextAdapterStore.getDataAdapters(context);
			for (final DataAdapter<?> a : adapters) {
				if (!persistentAdapterStore.adapterExists(a.getAdapterId())) {
					persistentAdapterStore.addAdapter(a);
				}
			}
			final IndexStore persistentIndexStore = GeoWaveStoreFinder.createIndexStore(
					configOptions,
					namespace);
			final Index[] indices = JobContextIndexStore.getIndices(context);
			for (final Index i : indices) {
				if (!persistentIndexStore.indexExists(i.getId())) {
					persistentIndexStore.addIndex(i);
				}
			}
			final AdapterStore jobContextAdapterStore = new JobContextAdapterStore(
					context,
					persistentAdapterStore);
			final IndexStore jobContextIndexStore = new JobContextIndexStore(
					context,
					persistentIndexStore);
			final DataStore dataStore = GeoWaveStoreFinder.createDataStore(
					configOptions,
					namespace);
			return new GeoWaveRecordWriter(
					context,
					dataStore,
					jobContextIndexStore,
					jobContextAdapterStore);
		}
		catch (final Exception e) {
			throw new IOException(
					e);
		}
	}

	public static void setDataStoreName(
			final Configuration config,
			final String dataStoreName ) {
		GeoWaveConfiguratorBase.setDataStoreName(
				CLASS,
				config,
				dataStoreName);
	}

	public static void setDataStatisticsStoreName(
			final Configuration config,
			final String dataStatisticsStoreName ) {
		GeoWaveConfiguratorBase.setDataStatisticsStoreName(
				CLASS,
				config,
				dataStatisticsStoreName);
	}

	public static void setIndexStoreName(
			final Configuration config,
			final String indexStoreName ) {
		GeoWaveConfiguratorBase.setIndexStoreName(
				CLASS,
				config,
				indexStoreName);
	}

	public static void setAdapterStoreName(
			final Configuration config,
			final String dataStoreName ) {
		GeoWaveConfiguratorBase.setAdapterStoreName(
				CLASS,
				config,
				dataStoreName);
	}

	public static void setGeoWaveNamespace(
			final Configuration config,
			final String namespace ) {
		GeoWaveConfiguratorBase.setGeoWaveNamespace(
				CLASS,
				config,
				namespace);
	}

	public static void setStoreConfigOptions(
			final Configuration config,
			final Map<String, String> storeConfigOptions ) {
		GeoWaveConfiguratorBase.setStoreConfigOptions(
				CLASS,
				config,
				storeConfigOptions);
	}

	public static void addIndex(
			final Configuration config,
			final PrimaryIndex index ) {
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

	protected static IndexStore getJobContextIndexStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextIndexStore(
				CLASS,
				context);
	}

	protected static AdapterStore getJobContextAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterStore(
				CLASS,
				context);
	}

	public static String getGeoWaveNamespace(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getGeoWaveNamespace(
				CLASS,
				context);
	}

	public static Map<String, String> getStoreConfigOptions(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreConfigOptions(
				CLASS,
				context);
	}

	public static String getDataStoreName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getDataStoreName(
				CLASS,
				context);
	}

	public static String getIndexStoreName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getIndexStoreName(
				CLASS,
				context);
	}

	public static String getDataStatisticsStoreName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getDataStatisticsStoreName(
				CLASS,
				context);
	}

	public static String getAdapterStoreName(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getAdapterStoreName(
				CLASS,
				context);
	}

	@Override
	public void checkOutputSpecs(
			final JobContext context )
			throws IOException,
			InterruptedException {
		// attempt to get each of the GeoWave stores from the job context
		try {
			final String namespace = getGeoWaveNamespace(context);

			final Map<String, Object> configOptions = ConfigUtils.valuesFromStrings(getStoreConfigOptions(context));
			if (GeoWaveStoreFinder.createDataStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave data store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createIndexStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave index store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createAdapterStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave adapter store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createDataStatisticsStore(
					configOptions,
					namespace) == null) {
				final String msg = "Unable to find GeoWave data statistics store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Error finding GeoWave stores",
					e);
			throw new IOException(
					"Error finding GeoWave stores",
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
				final DataStore dataStore,
				final IndexStore indexStore,
				final AdapterStore adapterStore ) {
			this.dataStore = dataStore;
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
				for (final ByteArrayId indexId : ingestKey.getIndexIds()) {
					final IndexWriter indexWriter = getIndexWriter(indexId);
					if (indexWriter != null) {
						indexWriter.write(
								(WritableDataAdapter) adapter,
								object);
					}
					else {
						LOGGER.warn("Cannot write to index '" + StringUtils.stringFromBinary(ingestKey.getAdapterId().getBytes()) + "'");
					}
				}
			}
			else {
				LOGGER.warn("Adapter '" + StringUtils.stringFromBinary(ingestKey.getAdapterId().getBytes()) + "' is not writable");
			}
		}

		private synchronized IndexWriter getIndexWriter(
				final ByteArrayId indexId ) {
			if (!indexWriterCache.containsKey(indexId)) {
				final PrimaryIndex index = (PrimaryIndex) indexStore.getIndex(indexId);
				IndexWriter writer = null;
				if (index != null) {
					writer = dataStore.createIndexWriter(
							index,
							DataStoreUtils.DEFAULT_VISIBILITY);
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

}
