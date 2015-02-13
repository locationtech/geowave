package mil.nga.giat.geowave.accumulo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsBuilder;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

/**
 * This class can write many entries for a single index by retaining a single
 * open writer. The first entry that is written will open a writer and it is the
 * responsibility of the caller to close this writer when complete.
 *
 */
public class AccumuloIndexWriter implements
		IndexWriter
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloIndexWriter.class);
	protected final Index index;
	protected final AccumuloOperations accumuloOperations;
	protected final AccumuloOptions accumuloOptions;
	protected final AccumuloDataStore dataStore;
	protected Writer writer;
	protected Writer altIdxWriter;

	protected boolean useAltIndex;
	protected String indexName;
	protected String altIdxTableName;

	protected boolean persistStats;
	protected final Map<ByteArrayId, List<DataStatisticsBuilder>> statsMap = new HashMap<ByteArrayId, List<DataStatisticsBuilder>>();

	public AccumuloIndexWriter(
			final Index index,
			final AccumuloOperations accumuloOperations,
			final AccumuloDataStore dataStore ) {
		this(
				index,
				accumuloOperations,
				new AccumuloOptions(),
				dataStore);
	}

	public AccumuloIndexWriter(
			final Index index,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions,
			final AccumuloDataStore dataStore ) {
		this.index = index;
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		this.dataStore = dataStore;
		initialize();
	}

	private void initialize() {
		indexName = StringUtils.stringFromBinary(index.getId().getBytes());
		altIdxTableName = indexName + AccumuloUtils.ALT_INDEX_TABLE;

		useAltIndex = accumuloOptions.isUseAltIndex();
		persistStats = accumuloOptions.isPersistDataStatistics();
		if (useAltIndex) {
			if (accumuloOperations.tableExists(indexName)) {
				if (!accumuloOperations.tableExists(altIdxTableName)) {
					useAltIndex = false;
					LOGGER.warn("Requested alternate index table [" + altIdxTableName + "] does not exist.");
				}
			}
			else {
				if (accumuloOperations.tableExists(altIdxTableName)) {
					accumuloOperations.deleteTable(altIdxTableName);
					LOGGER.warn("Deleting current alternate index table [" + altIdxTableName + "] as main table does not yet exist.");
				}
			}
		}
	}

	private synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = accumuloOperations.createWriter(
						StringUtils.stringFromBinary(index.getId().getBytes()),
						accumuloOptions.isCreateTable());
			}
			catch (final TableNotFoundException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
		if (useAltIndex && (altIdxWriter == null)) {
			try {
				altIdxWriter = accumuloOperations.createWriter(
						altIdxTableName,
						accumuloOptions.isCreateTable());
			}
			catch (final TableNotFoundException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}

	private synchronized void closeInternal() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
		if (useAltIndex && (altIdxWriter != null)) {
			altIdxWriter.close();
			altIdxWriter = null;
		}
	}

	@Override
	public Index getIndex() {
		return index;
	}

	@Override
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry ) {
		if (writableAdapter instanceof IndexDependentDataAdapter) {
			final IndexDependentDataAdapter adapter = ((IndexDependentDataAdapter) writableAdapter);
			final Iterator<T> indexedEntries = adapter.convertToIndex(
					index,
					entry);
			final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
			while (indexedEntries.hasNext()) {
				rowIds.addAll(writeInternal(
						adapter,
						indexedEntries.next()));
			}
			return rowIds;
		}
		else {
			return writeInternal(
					writableAdapter,
					entry);
		}
	}

	public <T> List<ByteArrayId> writeInternal(
			final WritableDataAdapter<T> writableAdapter,
			final T entry ) {
		final ByteArrayId adapterIdObj = writableAdapter.getAdapterId();

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();

		try {
			if (writableAdapter instanceof AttachedIteratorDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						AttachedIteratorDataAdapter.ATTACHED_ITERATOR_CACHE_ID).add(
						adapterIdObj,
						indexName)) {
					accumuloOperations.attachIterators(
							indexName,
							accumuloOptions.isCreateTable(),
							((AttachedIteratorDataAdapter) writableAdapter).getAttachedIteratorConfig(index));
				}
			}
			if (accumuloOptions.isUseLocalityGroups() && !accumuloOperations.localityGroupExists(
					indexName,
					adapterId)) {
				accumuloOperations.addLocalityGroup(
						indexName,
						adapterId);
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + writableAdapter.getAdapterId().getString() + "]",
					e);
		}
		DataStoreEntryInfo entryInfo;
		synchronized (this) {
			dataStore.store(writableAdapter);
			dataStore.store(index);

			ensureOpen();
			entryInfo = AccumuloUtils.write(
					writableAdapter,
					index,
					entry,
					writer);

			if (useAltIndex) {
				AccumuloUtils.writeAltIndex(
						writableAdapter,
						entryInfo,
						entry,
						altIdxWriter);
			}
			if (persistStats) {
				List<DataStatisticsBuilder> stats;
				if (statsMap.containsKey(adapterIdObj)) {
					stats = statsMap.get(adapterIdObj);
				}
				else {
					if (writableAdapter instanceof StatisticalDataAdapter) {
						final ByteArrayId[] statisticsIds = ((StatisticalDataAdapter<T>) writableAdapter).getSupportedStatisticsIds();
						stats = new ArrayList<DataStatisticsBuilder>(
								statisticsIds.length);
						for (final ByteArrayId id : statisticsIds) {
							stats.add(new DataStatisticsBuilder<T>(
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

	@Override
	public void close() {
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
								accumulatedStats.addAll(s);
							}
						}
					}
				}
			}
			if (!accumulatedStats.isEmpty()) {
				final DataStatisticsStore statsStore = new AccumuloDataStatisticsStore(
						accumuloOperations);
				for (final DataStatistics s : accumulatedStats) {
					statsStore.incorporateStatistics(s);
				}
			}
		}
	}

	@Override
	public <T> void setupAdapter(
			final WritableDataAdapter<T> writableAdapter ) {
		try {
			final ByteArrayId adapterIdObj = writableAdapter.getAdapterId();

			final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
			if (writableAdapter instanceof AttachedIteratorDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						AttachedIteratorDataAdapter.ATTACHED_ITERATOR_CACHE_ID).add(
						adapterIdObj,
						indexName)) {
					accumuloOperations.attachIterators(
							indexName,
							accumuloOptions.isCreateTable(),
							((AttachedIteratorDataAdapter) writableAdapter).getAttachedIteratorConfig(index));
				}
			}
			if (accumuloOptions.isUseLocalityGroups() && !accumuloOperations.localityGroupExists(
					indexName,
					adapterId)) {
				accumuloOperations.addLocalityGroup(
						indexName,
						adapterId);
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + writableAdapter.getAdapterId().getString() + "]",
					e);
		}
	}
}
