package mil.nga.giat.geowave.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.accumulo.query.AccumuloRowIdQuery;
import mil.nga.giat.geowave.accumulo.query.AccumuloRowPrefixQuery;
import mil.nga.giat.geowave.accumulo.query.SingleEntryFilterIterator;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.util.AltIndexIngestCallback;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper.Callback;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper.Converter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.IngestCallback;
import mil.nga.giat.geowave.store.IngestCallbackList;
import mil.nga.giat.geowave.store.IngestEntryInfo;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsBuilder;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.store.adapter.statistics.StatisticalDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.query.Query;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

/**
 * This is the Accumulo implementation of the data store. It requires an
 * AccumuloOperations instance that describes how to connect (read/write data)
 * to Apache Accumulo. It can create default implementations of the IndexStore
 * and AdapterStore based on the operations which will persist configuration
 * information to Accumulo tables, or an implementation of each of these stores
 * can be passed in.
 *
 * A DataStore can both ingest and query data based on persisted indices and
 * data adapters. When the data is ingested it is explicitly given an index and
 * a data adapter which is then persisted to be used in subsequent queries.
 */
public class AccumuloDataStore implements
		DataStore
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStore.class);

	protected final IndexStore indexStore;
	protected final AdapterStore adapterStore;
	protected final DataStatisticsStore statisticsStore;
	protected final AccumuloOperations accumuloOperations;
	protected final AccumuloOptions accumuloOptions;

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations ) {
		this(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);
	}

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		this(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations,
				accumuloOptions);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloOperations accumuloOperations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
	}

	@Override
	public <T> IndexWriter createIndexWriter(
			final Index index ) {
		return new AccumuloIndexWriter(
				index,
				accumuloOperations,
				accumuloOptions,
				this);
	}

	@Override
	public <T> List<ByteArrayId> ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry ) {
		if (writableAdapter instanceof IndexDependentDataAdapter) {
			final IndexDependentDataAdapter adapter = ((IndexDependentDataAdapter) writableAdapter);
			final Iterator<T> indexedEntries = adapter.convertToIndex(
					index,
					entry);
			final List<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();
			while (indexedEntries.hasNext()) {
				rowIds.addAll(ingestInternal(
						adapter,
						index,
						indexedEntries.next()));
			}
			return rowIds;
		}
		else {
			return ingestInternal(
					writableAdapter,
					index,
					entry);
		}
	}

	public <T> List<ByteArrayId> ingestInternal(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry ) {
		store(writableAdapter);
		store(index);

		Writer writer;
		try {
			final String indexName = StringUtils.stringFromBinary(index.getId().getBytes());
			final String altIdxTableName = indexName + AccumuloUtils.ALT_INDEX_TABLE;
			final byte[] adapterId = writableAdapter.getAdapterId().getBytes();

			boolean useAltIndex = accumuloOptions.isUseAltIndex();

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
			boolean persistStats = accumuloOptions.isPersistDataStatistics() && (writableAdapter instanceof StatisticalDataAdapter) && (statisticsStore != null);
			List<DataStatisticsBuilder<T>> statisticsBuilders = null;
			if (persistStats) {
				final ByteArrayId[] statisticsIds = ((StatisticalDataAdapter<T>) writableAdapter).getSupportedStatisticsIds();
				if ((statisticsIds != null) && (statisticsIds.length != 0)) {
					persistStats = true;
					statisticsBuilders = new ArrayList<DataStatisticsBuilder<T>>(
							statisticsIds.length);
					for (final ByteArrayId id : statisticsIds) {
						statisticsBuilders.add(new DataStatisticsBuilder<T>(
								(StatisticalDataAdapter) writableAdapter,
								id));
					}
				}
				else {
					persistStats = false;
				}
			}
			writer = accumuloOperations.createWriter(
					indexName,
					accumuloOptions.isCreateTable());

			if (accumuloOptions.isUseLocalityGroups() && !accumuloOperations.localityGroupExists(
					indexName,
					adapterId)) {
				accumuloOperations.addLocalityGroup(
						indexName,
						adapterId);
			}
			if (writableAdapter instanceof AttachedIteratorDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						AttachedIteratorDataAdapter.ATTACHED_ITERATOR_CACHE_ID).add(
						writableAdapter.getAdapterId(),
						indexName)) {
					accumuloOperations.attachIterators(
							indexName,
							accumuloOptions.isCreateTable(),
							((AttachedIteratorDataAdapter) writableAdapter).getAttachedIteratorConfig());
				}
			}
			final IngestEntryInfo entryInfo = AccumuloUtils.write(
					writableAdapter,
					index,
					entry,
					writer);

			writer.close();

			if (useAltIndex) {

				final Writer altIdxWriter = accumuloOperations.createWriter(
						altIdxTableName,
						accumuloOptions.isCreateTable());

				AccumuloUtils.writeAltIndex(
						writableAdapter,
						entryInfo,
						entry,
						altIdxWriter);

				altIdxWriter.close();
			}
			if (persistStats) {
				for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
					builder.entryIngested(
							entryInfo,
							entry);
					final Collection<DataStatistics<T>> statistics = builder.getStatistics();
					for (final DataStatistics<T> s : statistics) {
						statisticsStore.incorporateStatistics(s);
					}
				}
			}

			return entryInfo.getRowIds();
		}
		catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to ingest data entry",
					e);
		}
		return new ArrayList<ByteArrayId>();
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (accumuloOptions.isPersistAdapter() && !adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
	}

	protected synchronized void store(
			final Index index ) {
		if (accumuloOptions.isPersistIndex() && !indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}

	@Override
	public <T> void ingest(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator ) {
		ingest(
				dataWriter,
				index,
				entryIterator,
				null);
	}

	@Override
	public <T> void ingest(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback ) {
		if (dataWriter instanceof IndexDependentDataAdapter) {
			ingest(
					dataWriter,
					index,
					new IteratorWrapper<T, T>(
							entryIterator,
							new Converter<T, T>() {

								@Override
								public Iterator<T> convert(
										final T entry ) {
									return ((IndexDependentDataAdapter) dataWriter).convertToIndex(
											index,
											entry);
								}
							},
							null),
					ingestCallback);
		}
		else {
			ingestInternal(
					dataWriter,
					index,
					entryIterator,
					ingestCallback);
		}
	}

	private <T> void ingestInternal(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback ) {
		try {
			store(dataWriter);
			store(index);

			final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
			final String altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;
			final byte[] adapterId = dataWriter.getAdapterId().getBytes();

			boolean useAltIndex = accumuloOptions.isUseAltIndex();

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
			}
			final String indexName = StringUtils.stringFromBinary(index.getId().getBytes());
			final mil.nga.giat.geowave.accumulo.Writer writer = accumuloOperations.createWriter(
					indexName,
					accumuloOptions.isCreateTable());

			if (accumuloOptions.isUseLocalityGroups() && !accumuloOperations.localityGroupExists(
					tableName,
					adapterId)) {
				accumuloOperations.addLocalityGroup(
						tableName,
						adapterId);
			}
			if (dataWriter instanceof AttachedIteratorDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						AttachedIteratorDataAdapter.ATTACHED_ITERATOR_CACHE_ID).add(
						dataWriter.getAdapterId(),
						indexName)) {
					accumuloOperations.attachIterators(
							indexName,
							accumuloOptions.isCreateTable(),
							((AttachedIteratorDataAdapter) dataWriter).getAttachedIteratorConfig());
				}
			}
			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();
			Writer altIdxWriter = null;
			if (useAltIndex) {
				altIdxWriter = accumuloOperations.createWriter(
						altIdxTableName,
						accumuloOptions.isCreateTable());

				callbacks.add(new AltIndexIngestCallback<T>(
						altIdxWriter,
						dataWriter));
			}
			boolean persistStats = accumuloOptions.isPersistDataStatistics() && (dataWriter instanceof StatisticalDataAdapter) && (statisticsStore != null);
			List<DataStatisticsBuilder<T>> statisticsBuilders = null;
			if (persistStats) {
				final ByteArrayId[] statisticsIds = ((StatisticalDataAdapter<T>) dataWriter).getSupportedStatisticsIds();
				statisticsBuilders = new ArrayList<DataStatisticsBuilder<T>>(
						statisticsIds.length);
				for (final ByteArrayId id : statisticsIds) {
					statisticsBuilders.add(new DataStatisticsBuilder<T>(
							(StatisticalDataAdapter) dataWriter,
							id));
				}
				if ((statisticsBuilders != null) && !statisticsBuilders.isEmpty()) {
					callbacks.addAll(statisticsBuilders);
				}
				else {
					persistStats = false;
				}
			}
			if (ingestCallback != null) {
				callbacks.add(ingestCallback);
			}
			final IngestCallback<T> finalIngestCallback;
			if (callbacks.size() > 1) {
				finalIngestCallback = new IngestCallbackList<T>(
						callbacks);
			}
			else if (callbacks.size() == 1) {
				finalIngestCallback = callbacks.get(0);
			}
			else {
				finalIngestCallback = null;
			}

			writer.write(new Iterable<Mutation>() {
				@Override
				public Iterator<Mutation> iterator() {
					return new IteratorWrapper<T, Mutation>(
							entryIterator,
							new Converter<T, Mutation>() {

								@Override
								public Iterator<Mutation> convert(
										final T entry ) {
									return AccumuloUtils.entryToMutations(
											dataWriter,
											index,
											entry).iterator();
								}
							},
							finalIngestCallback == null ? null : new Callback<T, Mutation>() {

								@Override
								public void notifyIterationComplete(
										final T entry ) {
									finalIngestCallback.entryIngested(
											AccumuloUtils.getIngestInfo(
													dataWriter,
													index,
													entry),
											entry);
								}
							});
				}
			});
			writer.close();

			if (useAltIndex && (altIdxWriter != null)) {
				altIdxWriter.close();
			}
			if (persistStats) {
				for (final DataStatisticsBuilder<T> builder : statisticsBuilders) {
					final Collection<DataStatistics<T>> statistics = builder.getStatistics();
					for (final DataStatistics<T> s : statistics) {
						statisticsStore.incorporateStatistics(s);
					}
				}
			}
		}
		catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to ingest data entries",
					e);
		}
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Query query ) {
		return query(
				adapter,
				query,
				null);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Query query,
			final int limit ) {
		return query(
				adapter,
				query,
				new Integer(
						limit));
	}

	@SuppressWarnings("unchecked")
	private <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Query query,
			final Integer limit ) {
		store(adapter);
		return ((CloseableIterator<T>) query(
				Arrays.asList(new ByteArrayId[] {
					adapter.getAdapterId()
				}),
				query,
				new MemoryAdapterStore(
						new DataAdapter[] {
							adapter
						}),
				limit));
	}

	@Override
	public CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query,
			final int limit ) {
		return query(
				adapterIds,
				query,
				adapterStore,
				limit);
	}

	@Override
	public CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query ) {
		return query(
				adapterIds,
				query,
				adapterStore,
				null);
	}

	private CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query,
			final AdapterStore adapterStore,
			final Integer limit ) {
		// query the indices that are supported for this query object, and these
		// data adapter Ids
		try (final CloseableIterator<Index> indices = indexStore.getIndices()) {
			final List<CloseableIterator<?>> results = new ArrayList<CloseableIterator<?>>();
			while (indices.hasNext()) {
				final Index index = indices.next();
				final AccumuloConstraintsQuery accumuloQuery;
				if (query == null) {
					accumuloQuery = new AccumuloConstraintsQuery(
							adapterIds,
							index);
				}
				else if (query.isSupported(index)) {
					// construct the query
					accumuloQuery = new AccumuloConstraintsQuery(
							adapterIds,
							index,
							query.getIndexConstraints(index.getIndexStrategy()),
							query.createFilters(index.getIndexModel()));
				}
				else {
					continue;
				}
				results.add(accumuloQuery.query(
						accumuloOperations,
						adapterStore,
						limit));
			}
			// concatenate iterators
			return new CloseableIteratorWrapper<Object>(
					new Closeable() {
						@Override
						public void close()
								throws IOException {
							for (final CloseableIterator<?> result : results) {
								result.close();
							}
						}
					},
					Iterators.concat(results.iterator()));

		}
		catch (final IOException e) {
			LOGGER.warn(
					"unable to close index iterator for query",
					e);
		}
		return new CloseableIteratorWrapper<Object>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {}
				},
				new ArrayList<Object>().iterator());
	}

	protected static byte[] getRowIdBytes(
			final AccumuloRowId rowElements ) {
		final ByteBuffer buf = ByteBuffer.allocate(12 + rowElements.getDataId().length + rowElements.getAdapterId().length + rowElements.getIndexId().length);
		buf.put(rowElements.getIndexId());
		buf.put(rowElements.getAdapterId());
		buf.put(rowElements.getDataId());
		buf.putInt(rowElements.getAdapterId().length);
		buf.putInt(rowElements.getDataId().length);
		buf.putInt(rowElements.getNumberOfDuplicates());
		return buf.array();
	}

	protected static AccumuloRowId getRowIdObject(
			final byte[] row ) {
		final byte[] metadata = Arrays.copyOfRange(
				row,
				row.length - 12,
				row.length);
		final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				row,
				0,
				row.length - 12);
		final byte[] indexId = new byte[row.length - 12 - adapterIdLength - dataIdLength];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		buf.get(indexId);
		buf.get(adapterId);
		buf.get(dataId);
		return new AccumuloRowId(
				indexId,
				dataId,
				adapterId,
				numberOfDuplicates);
	}

	@Override
	@Deprecated
	@SuppressWarnings("unchecked")
	public <T> T getEntry(
			final Index index,
			final ByteArrayId rowId ) {
		final AccumuloRowIdQuery q = new AccumuloRowIdQuery(
				index,
				rowId);
		return (T) q.query(
				accumuloOperations,
				adapterStore);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getEntry(
			final Index index,
			final ByteArrayId dataId,
			final ByteArrayId adapterId ) {
		final String altIdxTableName = index.getId().getString() + AccumuloUtils.ALT_INDEX_TABLE;

		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(altIdxTableName)) {
			final List<ByteArrayId> rowIds = getAltIndexRowIds(
					altIdxTableName,
					dataId,
					adapterId);

			if ((rowIds != null) && !rowIds.isEmpty()) {
				final AccumuloRowIdQuery q = new AccumuloRowIdQuery(
						index,
						rowIds.get(0));
				return (T) q.query(
						accumuloOperations,
						adapterStore);
			}
		}
		else {
			final String tableName = index.getId().getString();
			final List<Entry<Key, Value>> rows = getEntryRows(
					tableName,
					dataId,
					adapterId);

			if ((rows != null) && !rows.isEmpty()) {
				for (final Entry<Key, Value> row : rows) {
					return (T) AccumuloUtils.decodeRow(
							row.getKey(),
							row.getValue(),
							adapterStore.getAdapter(adapterId),
							index);
				}
			}
		}
		return null;
	}

	@Override
	public boolean deleteEntry(
			final Index index,
			final ByteArrayId dataId,
			final ByteArrayId adapterId ) {
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;
		boolean success = true;

		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(altIdxTableName)) {
			final List<ByteArrayId> rowIds = getAltIndexRowIds(
					altIdxTableName,
					dataId,
					adapterId);

			if ((rowIds != null) && !rowIds.isEmpty()) {
				for (final ByteArrayId rowId : rowIds) {
					if (!accumuloOperations.delete(
							tableName,
							rowId,
							null,
							null)) {
						success = false;
					}
				}
			}
			else {
				success = false;
			}

			if (!deleteAltIndexEntry(
					altIdxTableName,
					dataId,
					adapterId)) {
				success = false;
			}
		}
		else {
			final List<Entry<Key, Value>> rows = getEntryRows(
					tableName,
					dataId,
					adapterId);

			if ((rows != null) && !rows.isEmpty()) {
				for (final Entry<Key, Value> row : rows) {
					final ByteArrayId rowId = new ByteArrayId(
							row.getKey().getRowData().getBackingArray());
					if (!accumuloOperations.delete(
							tableName,
							rowId,
							null,
							null)) {
						success = false;
					}
				}
			}
			else {
				success = false;
			}
		}
		return success;
	}

	protected List<Entry<Key, Value>> getEntryRows(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId ) {
		final List<Entry<Key, Value>> rows = new ArrayList<Entry<Key, Value>>();

		ScannerBase scanner = null;
		try {

			scanner = accumuloOperations.createScanner(tableName);

			scanner.fetchColumnFamily(new Text(
					adapterId.getBytes()));

			final IteratorSetting rowIteratorSettings = new IteratorSetting(
					SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
					SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
					WholeRowIterator.class);
			scanner.addScanIterator(rowIteratorSettings);

			final IteratorSetting filterIteratorSettings = new IteratorSetting(
					SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_PRIORITY,
					SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_NAME,
					SingleEntryFilterIterator.class);

			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.ADAPTER_ID,
					ByteArrayUtils.byteArrayToString(adapterId.getBytes()));

			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.DATA_ID,
					ByteArrayUtils.byteArrayToString(dataId.getBytes()));
			scanner.addScanIterator(filterIteratorSettings);

			final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();
				rows.add(entry);
			}

			scanner.close();
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
			return null;
		}

		return rows;
	}

	protected List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId ) {

		final ArrayList<ByteArrayId> rowIds = new ArrayList<ByteArrayId>();

		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(tableName)) {
			ScannerBase scanner = null;
			try {
				scanner = accumuloOperations.createScanner(tableName);

				((Scanner) scanner).setRange(Range.exact(new Text(
						dataId.getBytes())));

				scanner.fetchColumnFamily(new Text(
						adapterId.getBytes()));

				final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
				while (iterator.hasNext()) {
					final Entry<Key, Value> entry = iterator.next();

					final ByteArrayId rowId = new ByteArrayId(
							entry.getKey().getColumnQualifierData().getBackingArray());

					rowIds.add(rowId);
				}

				scanner.close();
			}
			catch (final TableNotFoundException e) {
				LOGGER.warn(
						"Unable to query table '" + tableName + "'.  Table does not exist.",
						e);
				return null;
			}
		}

		return rowIds;
	}

	protected boolean deleteAltIndexEntry(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId ) {
		boolean success = true;
		BatchDeleter deleter = null;
		try {

			deleter = accumuloOperations.createBatchDeleter(tableName);

			deleter.setRanges(Arrays.asList(Range.exact(new Text(
					dataId.getBytes()))));

			deleter.fetchColumnFamily(new Text(
					adapterId.getBytes()));

			final Iterator<Map.Entry<Key, Value>> iterator = deleter.iterator();
			while (iterator.hasNext()) {
				final Entry<Key, Value> entry = iterator.next();

				if (!(Arrays.equals(
						entry.getKey().getRowData().getBackingArray(),
						dataId.getBytes()) && Arrays.equals(
						entry.getKey().getColumnFamilyData().getBackingArray(),
						adapterId.getBytes()))) {
					success = false;
					break;
				}
			}

			if (success) {
				deleter.delete();
			}

			deleter.close();
		}
		catch (final TableNotFoundException | MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to delete entries from alternate index table [" + tableName + "].",
					e);
			if (deleter != null) {
				deleter.close();
			}
			success = false;
		}

		return success;
	}

	@Override
	public CloseableIterator<?> getEntriesByPrefix(
			final Index index,
			final ByteArrayId rowPrefix ) {
		final AccumuloRowPrefixQuery q = new AccumuloRowPrefixQuery(
				index,
				rowPrefix);
		return q.query(
				accumuloOperations,
				adapterStore);
	}

	@Override
	public CloseableIterator<?> query(
			final Query query ) {
		return query(
				(List<ByteArrayId>) null,
				query);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final Index index,
			final Query query ) {
		return query(
				index,
				query,
				null);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final Index index,
			final Query query,
			final int limit ) {
		return query(
				index,
				query,
				(Integer) limit);
	}

	@Override
	public CloseableIterator<?> query(
			final Query query,
			final int limit ) {
		return query(
				(List<ByteArrayId>) null,
				query,
				limit);
	}

	@SuppressWarnings("unchecked")
	private <T> CloseableIterator<T> query(
			final Index index,
			final Query query,
			final Integer limit ) {
		if (!query.isSupported(index)) {
			throw new IllegalArgumentException(
					"Index does not support the query");
		}
		final AccumuloConstraintsQuery q = new AccumuloConstraintsQuery(
				index,
				query.getIndexConstraints(index.getIndexStrategy()),
				query.createFilters(index.getIndexModel()));
		return (CloseableIterator<T>) q.query(
				accumuloOperations,
				adapterStore,
				limit);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final int limit ) {
		return query(
				adapter,
				index,
				query,
				(Integer) limit);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query ) {
		return query(
				adapter,
				index,
				query,
				null);
	}

	@SuppressWarnings("unchecked")
	private <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final Integer limit ) {
		if (!query.isSupported(index)) {
			throw new IllegalArgumentException(
					"Index does not support the query");
		}
		store(adapter);

		final AccumuloConstraintsQuery q = new AccumuloConstraintsQuery(
				Arrays.asList(new ByteArrayId[] {
					adapter.getAdapterId()
				}),
				index,
				query.getIndexConstraints(index.getIndexStrategy()),
				query.createFilters(index.getIndexModel()));
		return (CloseableIterator<T>) q.query(
				accumuloOperations,
				new MemoryAdapterStore(
						new DataAdapter[] {
							adapter
						}),
				limit);
	}
}
