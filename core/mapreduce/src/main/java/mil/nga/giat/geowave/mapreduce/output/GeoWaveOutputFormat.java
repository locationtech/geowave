package mil.nga.giat.geowave.mapreduce.output;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.mapreduce.JobContextIndexStore;

/**
 * This output format is the preferred mechanism for writing data to GeoWave
 * within a map-reduce job.
 */
public class GeoWaveOutputFormat extends
		OutputFormat<GeoWaveOutputKey<Object>, Object>
{
	private static final Class<?> CLASS = GeoWaveOutputFormat.class;
	protected static final Logger LOGGER = LoggerFactory.getLogger(CLASS);

	@Override
	public RecordWriter<GeoWaveOutputKey<Object>, Object> getRecordWriter(
			final TaskAttemptContext context )
			throws IOException,
			InterruptedException {
		try {
			final Map<String, String> configOptions = getStoreOptionsMap(context);
			final AdapterStore persistentAdapterStore = GeoWaveStoreFinder.createAdapterStore(configOptions);
			final DataAdapter<?>[] adapters = JobContextAdapterStore.getDataAdapters(context);
			for (final DataAdapter<?> a : adapters) {
				if (!persistentAdapterStore.adapterExists(a.getAdapterId())) {
					persistentAdapterStore.addAdapter(a);
				}
			}

			final IndexStore persistentIndexStore = GeoWaveStoreFinder.createIndexStore(configOptions);
			final Index[] indices = JobContextIndexStore.getIndices(context);
			if (LOGGER.isDebugEnabled()) {
				final StringBuilder sbDebug = new StringBuilder();

				sbDebug.append("Config Options: ");
				for (final Map.Entry<String, String> entry : configOptions.entrySet()) {
					sbDebug.append(entry.getKey() + "/" + entry.getValue() + ", ");
				}
				sbDebug.append("\n\tIndices Size: " + indices.length);
				sbDebug.append("\n\tpersistentIndexStore: " + persistentIndexStore);
				final String filename = "/META-INF/services/mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi";

				final InputStream is = context.getClass().getResourceAsStream(
						filename);
				if (is == null) {
					sbDebug.append("\n\tStoreFactoryFamilySpi: Unable to open file '" + filename + "'");
				}
				else {
					sbDebug.append("\n\tStoreFactoryFamilySpi: " + IOUtils.toString(
							is,
							"UTF-8"));
				}

				LOGGER.debug(sbDebug.toString());
			}

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
			final DataStore dataStore = GeoWaveStoreFinder.createDataStore(configOptions);
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

	public static void setStoreOptions(
			final Configuration config,
			final DataStorePluginOptions storeOptions ) {
		if (storeOptions != null) {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					storeOptions.getOptionsAsMap());
		}
		else {
			GeoWaveConfiguratorBase.setStoreOptionsMap(
					CLASS,
					config,
					null);
		}
	}

	public static void setStoreOptionsMap(
			final Configuration config,
			final Map<String, String> storeConfigOptions ) {
		GeoWaveConfiguratorBase.setStoreOptionsMap(
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

	public static IndexStore getJobContextIndexStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextIndexStore(
				CLASS,
				context);
	}

	public static AdapterStore getJobContextAdapterStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterStore(
				CLASS,
				context);
	}

	public static AdapterIndexMappingStore getJobContextAdapterIndexMappingStore(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getJobContextAdapterIndexMappingStore(
				CLASS,
				context);
	}

	public static DataStorePluginOptions getStoreOptions(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreOptions(
				CLASS,
				context);
	}

	public static Map<String, String> getStoreOptionsMap(
			final JobContext context ) {
		return GeoWaveConfiguratorBase.getStoreOptionsMap(
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
			final Map<String, String> configOptions = getStoreOptionsMap(context);
			if (GeoWaveStoreFinder.createDataStore(configOptions) == null) {
				final String msg = "Unable to find GeoWave data store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createIndexStore(configOptions) == null) {
				final String msg = "Unable to find GeoWave index store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createAdapterStore(configOptions) == null) {
				final String msg = "Unable to find GeoWave adapter store";
				LOGGER.warn(msg);
				throw new IOException(
						msg);
			}
			if (GeoWaveStoreFinder.createDataStatisticsStore(configOptions) == null) {
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
			RecordWriter<GeoWaveOutputKey<Object>, Object>
	{
		private final Map<ByteArrayId, IndexWriter> adapterIdToIndexWriterCache = new HashMap<ByteArrayId, IndexWriter>();
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
				final Object data )
				throws IOException {
			boolean success = false;
			String errorMessage = null;

			if (ingestKey.getIndexIds().isEmpty()) {
				throw new IOException(
						"Empty index ID input list");
			}

			final WritableDataAdapter<?> adapter = ingestKey.getAdapter(adapterStore);
			if (adapter != null) {
				final IndexWriter indexWriter = getIndexWriter(
						adapter,
						ingestKey.getIndexIds());
				if (indexWriter != null) {
					List writeList = indexWriter.write(data);

					if (!writeList.isEmpty()) {
						success = true;
					}
					else {
						errorMessage = "Empty write list";
					}
				}
				else {
					errorMessage = "Cannot write to index '"
							+ StringUtils.stringFromBinary(ingestKey.getAdapterId().getBytes()) + "'";
				}
			}
			else {
				errorMessage = "Adapter '" + StringUtils.stringFromBinary(ingestKey.getAdapterId().getBytes())
						+ "' does not exist";
			}

			if (!success) {
				throw new IOException(
						errorMessage);
			}
		}

		private synchronized IndexWriter getIndexWriter(
				final DataAdapter<?> adapter,
				final Collection<ByteArrayId> indexIds )
				throws MismatchedIndexToAdapterMapping {
			IndexWriter writer = adapterIdToIndexWriterCache.get(adapter.getAdapterId());
			if (writer == null) {
				final List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>();
				for (final ByteArrayId indexId : indexIds) {
					final PrimaryIndex index = (PrimaryIndex) indexStore.getIndex(indexId);
					if (index != null) {
						indices.add(index);
					}
					else {
						LOGGER.warn("Index '" + StringUtils.stringFromBinary(indexId.getBytes()) + "' does not exist");
					}
				}

				writer = dataStore.createWriter(
						adapter,
						indices.toArray(new PrimaryIndex[indices.size()]));

				adapterIdToIndexWriterCache.put(
						adapter.getAdapterId(),
						writer);

			}
			return writer;
		}

		@Override
		public synchronized void close(
				final TaskAttemptContext attempt )
				throws IOException,
				InterruptedException {
			for (final IndexWriter indexWriter : adapterIdToIndexWriterCache.values()) {
				indexWriter.close();
			}
		}
	}

}
