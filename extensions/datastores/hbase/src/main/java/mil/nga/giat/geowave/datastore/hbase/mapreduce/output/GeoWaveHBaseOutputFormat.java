/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStore;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseConfiguratorBase;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.JobContextHBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.JobContextHBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

/**
 * @author viggy Functionality similar to <code> GeoWaveOutputFormat </code>
 */
public class GeoWaveHBaseOutputFormat extends
		OutputFormat<GeoWaveHBaseOutputKey, Object>
{

	private static final Class<?> CLASS = GeoWaveHBaseOutputFormat.class;
	protected static final Logger LOGGER = Logger.getLogger(
			CLASS);

	@Override
	public void checkOutputSpecs(
			final JobContext context )
					throws IOException,
					InterruptedException {
		try {
			if (getOperations(
					context) == null) {
				LOGGER.warn(
						"Zookeeper connection for hbase is null");
				throw new IOException(
						"Zookeeper connection for hbase is null");
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Error establishing zookeeper connection for hbase",
					e);
			throw new IOException(
					e);
		}
	}

	public static BasicHBaseOperations getOperations(
			final JobContext context )
					throws IOException {
		return GeoWaveHBaseConfiguratorBase.getOperations(
				CLASS,
				context);
	}

	@Override
	public OutputCommitter getOutputCommitter(
			final TaskAttemptContext context )
					throws IOException,
					InterruptedException {
		return new NullOutputFormat<ByteArrayId, Object>().getOutputCommitter(
				context);
	}

	@Override
	public RecordWriter<GeoWaveHBaseOutputKey, Object> getRecordWriter(
			final TaskAttemptContext context )
					throws IOException,
					InterruptedException {
		try {
			// TODO #406 expose GeoWave's HBaseOptions(Needs to be done for
			// accumulo also)
			final BasicHBaseOperations operations = getOperations(
					context);
			final AdapterStore adapterStore = new HBaseAdapterStore(
					operations);
			final DataAdapter<?>[] adapters = JobContextHBaseAdapterStore.getDataAdapters(
					context);
			for (final DataAdapter<?> a : adapters) {
				if (!adapterStore.adapterExists(
						a.getAdapterId())) {
					adapterStore.addAdapter(
							a);
				}
			}
			final IndexStore indexStore = new HBaseIndexStore(
					operations);
			final Index[] indices = JobContextHBaseIndexStore.getIndices(
					context);
			for (final Index i : indices) {
				if (!indexStore.indexExists(
						i.getId())) {
					indexStore.addIndex(
							i);
				}
			}
			final AdapterStore jobContextAdapterStore = getDataAdapterStore(
					context,
					operations);
			final IndexStore jobContextIndexStore = getIndexStore(
					context,
					operations);
			final DataStatisticsStore statisticsStore = new HBaseDataStatisticsStore(
					operations);
			return new GeoWaveHBaseRecordWriter(
					context,
					operations,
					jobContextIndexStore,
					jobContextAdapterStore,
					statisticsStore);
		}
		catch (final Exception e) {
			throw new IOException(
					e);
		}
	}

	protected static IndexStore getIndexStore(
			final JobContext context,
			final BasicHBaseOperations operations ) {
		return new JobContextHBaseIndexStore(
				context,
				operations);
	}

	protected static AdapterStore getDataAdapterStore(
			final JobContext context,
			final BasicHBaseOperations operations ) {
		return new JobContextHBaseAdapterStore(
				context,
				operations);
	}

	public static void setOperationsInfo(
			final Configuration config,
			final String zooKeepers,
			final String geowaveTableNamespace ) {
		GeoWaveHBaseConfiguratorBase.setZookeeperUrl(
				CLASS,
				config,
				zooKeepers);
		GeoWaveHBaseConfiguratorBase.setTableNamespace(
				CLASS,
				config,
				geowaveTableNamespace);
	}

	public static void setOperationsInfo(
			final Job job,
			final String zooKeepers,
			final String geowaveTableNamespace ) {
		setOperationsInfo(
				job.getConfiguration(),
				zooKeepers,
				geowaveTableNamespace);
	}

	protected static class GeoWaveHBaseRecordWriter extends
			RecordWriter<GeoWaveHBaseOutputKey, Object>
	{
		private final Map<ByteArrayId, IndexWriter> indexWriterCache = new HashMap<ByteArrayId, IndexWriter>();
		private final AdapterStore adapterStore;
		private final IndexStore indexStore;
		private final DataStore dataStore;

		protected GeoWaveHBaseRecordWriter(
				final TaskAttemptContext context,
				final BasicHBaseOperations operations,
				final IndexStore indexStore,
				final AdapterStore adapterStore,
				final DataStatisticsStore statisticsStore )
						throws IOException {
			final Level l = getLogLevel(
					context);
			if (l != null) {
				LOGGER.setLevel(
						getLogLevel(
								context));
			}
			dataStore = new HBaseDataStore(
					indexStore,
					adapterStore,
					statisticsStore,
					operations);
			this.adapterStore = adapterStore;
			this.indexStore = indexStore;
		}

		@SuppressWarnings({
			"unchecked",
			"rawtypes"
		})
		@Override
		public void write(
				final GeoWaveHBaseOutputKey ingestKey,
				final Object object )
						throws IOException {
			final DataAdapter<?> adapter = adapterStore.getAdapter(
					ingestKey.getAdapterId());
			if (adapter instanceof WritableDataAdapter) {
				final IndexWriter indexWriter = getIndexWriter(
						ingestKey.getIndexId());
				if (indexWriter != null) {
					indexWriter.write(
							(WritableDataAdapter) adapter,
							object);
				}
				else {
					LOGGER.warn(
							"Cannot write to index '" + StringUtils.stringFromBinary(
									ingestKey.getAdapterId().getBytes()) + "'");
				}
			}
			else {
				LOGGER.warn(
						"Adapter '" + StringUtils.stringFromBinary(
								ingestKey.getAdapterId().getBytes()) + "' is not writable");
			}
		}

		private synchronized IndexWriter getIndexWriter(
				final ByteArrayId indexId ) {
			if (!indexWriterCache.containsKey(
					indexId)) {
				final PrimaryIndex index = (PrimaryIndex) indexStore.getIndex(
						indexId);
				IndexWriter writer = null;
				if (index != null) {
					writer = dataStore.createIndexWriter(
							index,
							DataStoreUtils.DEFAULT_VISIBILITY);
				}
				else {
					LOGGER.warn(
							"Index '" + StringUtils.stringFromBinary(
									indexId.getBytes()) + "' does not exist");
				}
				indexWriterCache.put(
						indexId,
						writer);
				return writer;
			}
			return indexWriterCache.get(
					indexId);
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

	protected static Level getLogLevel(
			final JobContext context ) {
		// TODO #406 Need to fix
		LOGGER.warn(
				"Need to code this method getLogLevel1. Currently it is hardcoded");
		return Level.INFO;
	}

	public static void addIndex(
			final Configuration config,
			final PrimaryIndex index ) {
		JobContextHBaseIndexStore.addIndex(
				config,
				index);
	}

	public static void addDataAdapter(
			final Configuration config,
			final DataAdapter<?> adapter ) {
		JobContextHBaseAdapterStore.addDataAdapter(
				config,
				adapter);
	}

}
