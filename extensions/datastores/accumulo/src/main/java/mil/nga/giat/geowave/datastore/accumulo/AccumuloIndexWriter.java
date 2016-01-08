package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreCallbackManager;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.DataAdapterAndIndexCache;

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
	protected final PrimaryIndex index;
	protected final AccumuloOperations accumuloOperations;
	protected final AccumuloOptions accumuloOptions;
	protected final AccumuloDataStore dataStore;
	protected final DataStoreCallbackManager callbackCache;
	protected Writer writer;
	protected Writer altIdxWriter;

	protected boolean useAltIndex;
	protected String indexName;
	protected String altIdxTableName;

	// just need a reasonable threshold.

	protected final VisibilityWriter<?> customFieldVisibilityWriter;

	public AccumuloIndexWriter(
			final PrimaryIndex index,
			final AccumuloOperations accumuloOperations,
			final AccumuloDataStore dataStore,
			final DataStatisticsStore statsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final VisibilityWriter<?> customFieldVisibilityWriter ) {
		this(
				index,
				accumuloOperations,
				new AccumuloOptions(),
				dataStore,
				statsStore,
				secondaryIndexDataStore,
				customFieldVisibilityWriter);

	}

	public AccumuloIndexWriter(
			final PrimaryIndex index,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions,
			final AccumuloDataStore dataStore,
			final DataStatisticsStore statsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final VisibilityWriter<?> customFieldVisibilityWriter ) {
		this.index = index;
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		this.dataStore = dataStore;
		this.customFieldVisibilityWriter = customFieldVisibilityWriter;
		callbackCache = new DataStoreCallbackManager(
				statsStore,
				secondaryIndexDataStore);
		initialize();
	}

	private void initialize() {
		indexName = StringUtils.stringFromBinary(index.getId().getBytes());
		altIdxTableName = indexName + AccumuloUtils.ALT_INDEX_TABLE;

		useAltIndex = accumuloOptions.isUseAltIndex();
		callbackCache.setPersistStats(accumuloOptions.isPersistDataStatistics());
		if (useAltIndex) {
			if (accumuloOperations.tableExists(indexName)) {
				if (!accumuloOperations.tableExists(altIdxTableName)) {
					useAltIndex = false;
					LOGGER.info("Requested alternate index table [" + altIdxTableName + "] does not exist.");
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
						accumuloOptions.isCreateTable(),
						true,
						index.getIndexStrategy().getNaturalSplits());
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
						accumuloOptions.isCreateTable(),
						true,
						index.getIndexStrategy().getNaturalSplits());
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
	public PrimaryIndex getIndex() {
		return index;
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

	@Override
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry,
			final VisibilityWriter<T> feldVisibilityWriter ) {
		if (writableAdapter instanceof IndexDependentDataAdapter) {
			final IndexDependentDataAdapter adapter = ((IndexDependentDataAdapter) writableAdapter);
			final Iterator<T> indexedEntries = adapter.convertToIndex(
					index,
					entry);
			final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
			while (indexedEntries.hasNext()) {
				rowIds.addAll(writeInternal(
						adapter,
						indexedEntries.next(),
						feldVisibilityWriter));
			}
			return rowIds;
		}
		else {
			return writeInternal(
					writableAdapter,
					entry,
					feldVisibilityWriter);
		}
	}

	public <T> List<ByteArrayId> writeInternal(
			final WritableDataAdapter<T> writableAdapter,
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {
		final ByteArrayId adapterIdObj = writableAdapter.getAdapterId();

		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();

		try {
			if (writableAdapter instanceof RowMergingDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID).add(
						adapterIdObj,
						indexName)) {
					AccumuloUtils.attachRowMergingIterators(
							((RowMergingDataAdapter<?, ?>) writableAdapter),
							accumuloOperations,
							indexName,
							accumuloOptions.isCreateTable());
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
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;

		DataStoreEntryInfo entryInfo;
		synchronized (this) {
			dataStore.store(writableAdapter);
			dataStore.store(index);

			ensureOpen();
			if (writer == null) {
				return Collections.emptyList();
			}
			entryInfo = AccumuloUtils.write(
					writableAdapter,
					index,
					entry,
					writer,
					visibilityWriter);

			if (useAltIndex) {

				if (accumuloOperations.tableExists(tableName)) {
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

				AccumuloUtils.writeAltIndex(
						writableAdapter,
						entryInfo,
						entry,
						altIdxWriter);
			}
			callbackCache.getIngestCallback(
					writableAdapter,
					index).entryIngested(
					entryInfo,
					entry);
		}
		return entryInfo.getRowIds();
	}

	@Override
	public void close() {
		// thread safe close
		closeInternal();
		try {
			callbackCache.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot close callbacks",
					e);
		}
	}

	@Override
	public <T> void setupAdapter(
			final WritableDataAdapter<T> writableAdapter ) {
		try {
			final ByteArrayId adapterIdObj = writableAdapter.getAdapterId();

			final byte[] adapterId = writableAdapter.getAdapterId().getBytes();
			if (writableAdapter instanceof RowMergingDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID).add(
						adapterIdObj,
						indexName)) {
					AccumuloUtils.attachRowMergingIterators(
							((RowMergingDataAdapter<?, ?>) writableAdapter),
							accumuloOperations,
							indexName,
							accumuloOptions.isCreateTable());
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

	@Override
	public synchronized void flush() {
		// thread safe flush of the writers
		if (writer != null) {
			writer.flush();
		}
		if (useAltIndex && (altIdxWriter != null)) {
			altIdxWriter.flush();
		}
	}
}
