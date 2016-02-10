/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsBuilder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

/**
 * @author viggy Functionality similar to <code> AccumuloIndexWriter </code>
 */
public class HBaseIndexWriter implements
		IndexWriter
{

	private final static Logger LOGGER = Logger.getLogger(
			HBaseIndexWriter.class);
	private final PrimaryIndex index;
	private final HBaseDataStore dataStore;
	private final BasicHBaseOperations operations;
	private String indexName;
	protected boolean persistStats;
	protected HBaseWriter writer;
	protected HBaseWriter altIdxWriter;
	protected final HBaseOptions options;

	protected final Map<ByteArrayId, List<DataStatisticsBuilder>> statsMap = new HashMap<ByteArrayId, List<DataStatisticsBuilder>>();
	protected boolean useAltIndex;
	protected String altIdxTableName;

	protected final VisibilityWriter<?> customFieldVisibilityWriter;

	public HBaseIndexWriter(
			final PrimaryIndex index,
			final BasicHBaseOperations operations,
			final HBaseDataStore dataStore,
			final VisibilityWriter<?> customFieldVisibilityWriter ) {
		this(
				index,
				operations,
				new HBaseOptions(),
				dataStore,
				customFieldVisibilityWriter);
	}

	public HBaseIndexWriter(
			final PrimaryIndex index,
			final BasicHBaseOperations operations,
			final HBaseOptions options,
			final HBaseDataStore dataStore,
			final VisibilityWriter<?> customFieldVisibilityWriter ) {
		this.index = index;
		this.operations = operations;
		this.dataStore = dataStore;
		this.options = options;
		this.customFieldVisibilityWriter = customFieldVisibilityWriter;
		initialize();
	}

	private void initialize() {
		indexName = StringUtils.stringFromBinary(
				index.getId().getBytes());
		altIdxTableName = indexName + HBaseUtils.ALT_INDEX_TABLE;

		useAltIndex = options.isUseAltIndex();
		persistStats = options.isPersistDataStatistics();
		if (useAltIndex) {
			try {
				if (operations.tableExists(
						indexName)) {
					if (!operations.tableExists(
							altIdxTableName)) {
						useAltIndex = false;
						LOGGER.info(
								"Requested alternate index table [" + altIdxTableName + "] does not exist.");
					}
				}
				else {
					if (operations.tableExists(
							altIdxTableName)) {
						operations.deleteTable(
								altIdxTableName);
						LOGGER.warn(
								"Deleting current alternate index table [" + altIdxTableName + "] as main table does not yet exist.");
					}
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to check if Table " + indexName + " exists");
			}
		}
	}

	@Override
	public void close()
			throws IOException {
		// thread safe close
		closeInternal();

		// write the statistics
		if (persistStats) {
			final List<DataStatistics> accumulatedStats = new ArrayList<DataStatistics>();
			synchronized (this) {
				for (final List<DataStatisticsBuilder> builders : statsMap.values()) {
					if ((builders != null) && !builders.isEmpty()) {
						for (final DataStatisticsBuilder builder : builders) {
							final Collection<DataStatistics> s = builder.getStatistics();
							if ((s != null) && !s.isEmpty()) {
								accumulatedStats.addAll(
										s);
							}
						}
					}
				}
			}
			if (!accumulatedStats.isEmpty()) {
				final DataStatisticsStore statsStore = new HBaseDataStatisticsStore(
						operations);
				for (final DataStatistics s : accumulatedStats) {
					statsStore.incorporateStatistics(
							s);
				}
			}
		}
	}

	private synchronized void closeInternal() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

	@Override
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry ) {
		return write(
				writableAdapter,
				entry,
				(VisibilityWriter<T>) customFieldVisibilityWriter);
	}

	public <T> List<ByteArrayId> writeInternal(
			final WritableDataAdapter<T> writableAdapter,
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {
		final ByteArrayId adapterIdObj = writableAdapter.getAdapterId();

		// final byte[] adapterId = writableAdapter.getAdapterId().getBytes();

		DataStoreEntryInfo entryInfo;
		synchronized (this) {
			dataStore.store(
					writableAdapter);
			dataStore.store(
					index);

			ensureOpen(
					writableAdapter);
			entryInfo = HBaseUtils.write(
					writableAdapter,
					index,
					entry,
					writer,
					visibilityWriter);
			if (persistStats) {
				List<DataStatisticsBuilder> stats;
				if (statsMap.containsKey(
						adapterIdObj)) {
					stats = statsMap.get(
							adapterIdObj);
				}
				else {
					if (writableAdapter instanceof StatisticalDataAdapter) {
						final ByteArrayId[] statisticsIds = ((StatisticalDataAdapter<T>) writableAdapter).getSupportedStatisticsIds();
						stats = new ArrayList<DataStatisticsBuilder>(
								statisticsIds.length);
						for (final ByteArrayId id : statisticsIds) {
							stats.add(
									new DataStatisticsBuilder<T>(
											(StatisticalDataAdapter) writableAdapter,
											id));
						}
						if ((stats != null) && stats.isEmpty()) {
							// if its an empty list, for simplicity just set it
							// to null
							stats = null;
						}
					}
					else {
						stats = null;
					}
					statsMap.put(
							adapterIdObj,
							stats);
				}
				if (stats != null) {
					for (final DataStatisticsBuilder<T> s : stats) {
						s.entryIngested(
								entryInfo,
								entry);
					}
				}
			}
		}
		return entryInfo.getRowIds();
	}

	private synchronized <T> void ensureOpen(
			final WritableDataAdapter<T> writableAdapter ) {
		if (writer == null) {
			try {
				writer = operations.createWriter(
						StringUtils.stringFromBinary(
								index.getId().getBytes()),
						writableAdapter.getAdapterId().getString());
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}

	@Override
	public PrimaryIndex getIndex() {
		return index;
	}

	@Override
	public <T> void setupAdapter(
			final WritableDataAdapter<T> writableAdapter ) {
		LOGGER.error(
				"This method is not yet coded. Need to fix it");

	}

	@Override
	public synchronized void flush() {
		// HBase writer does not require/support flush
	}

	@Override
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter ) {
		if (writableAdapter instanceof IndexDependentDataAdapter) {
			final IndexDependentDataAdapter adapter = ((IndexDependentDataAdapter) writableAdapter);
			final Iterator<T> indexedEntries = adapter.convertToIndex(
					index,
					entry);
			final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
			while (indexedEntries.hasNext()) {
				rowIds.addAll(
						writeInternal(
								adapter,
								indexedEntries.next(),
								fieldVisibilityWriter));
			}
			return rowIds;
		}
		else {
			return writeInternal(
					writableAdapter,
					entry,
					fieldVisibilityWriter);
		}
	}

}
