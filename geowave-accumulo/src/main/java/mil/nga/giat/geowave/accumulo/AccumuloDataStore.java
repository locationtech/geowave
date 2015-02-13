package mil.nga.giat.geowave.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.accumulo.query.AccumuloFilteredIndexQuery;
import mil.nga.giat.geowave.accumulo.query.AccumuloRowIdQuery;
import mil.nga.giat.geowave.accumulo.query.AccumuloRowPrefixQuery;
import mil.nga.giat.geowave.accumulo.query.QueryFilterIterator;
import mil.nga.giat.geowave.accumulo.query.SingleEntryFilterIterator;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.accumulo.util.AltIndexIngestCallback;
import mil.nga.giat.geowave.accumulo.util.CloseableIteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper.Callback;
import mil.nga.giat.geowave.accumulo.util.IteratorWrapper.Converter;
import mil.nga.giat.geowave.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.IngestCallback;
import mil.nga.giat.geowave.store.IngestCallbackList;
import mil.nga.giat.geowave.store.ScanCallback;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.store.data.VisibilityWriter;
import mil.nga.giat.geowave.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.store.filter.MultiIndexDedupeFilter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.query.Query;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This is the Accumulo implementation of the data store. It requires an
 * AccumuloOperations instance that describes how to connect (read/write data)
 * to Apache Accumulo. It can create default implementations of the IndexStore
 * and AdapterStore based on the operations which will persist configuration
 * information to Accumulo tables, or an implementation of each of these stores
 * can be passed in A DataStore can both ingest and query data based on
 * persisted indices and data adapters. When the data is ingested it is
 * explicitly given an index and a data adapter which is then persisted to be
 * used in subsequent queries.
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
		return this.ingest(
				writableAdapter,
				index,
				entry,
				new UniformVisibilityWriter<T>(
						new UnconstrainedVisibilityHandler<T, Object>()));
	}

	@Override
	public <T> List<ByteArrayId> ingest(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
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
						indexedEntries.next(),
						customFieldVisibilityWriter));
			}
			return rowIds;
		}
		else {
			return ingestInternal(
					writableAdapter,
					index,
					entry,
					customFieldVisibilityWriter);
		}
	}

	public <T> List<ByteArrayId> ingestInternal(
			final WritableDataAdapter<T> writableAdapter,
			final Index index,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
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

			final StatsCompositionTool<T> statisticsTool = getStatsCompositionTool(writableAdapter);

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
							((AttachedIteratorDataAdapter) writableAdapter).getAttachedIteratorConfig(index));
				}
			}
			final DataStoreEntryInfo entryInfo = AccumuloUtils.write(
					writableAdapter,
					index,
					entry,
					writer,
					customFieldVisibilityWriter);

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

			statisticsTool.entryIngested(
					entryInfo,
					entry);
			
			statisticsTool.flush();

			return entryInfo.getRowIds();
		}
		catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to ingest data entry",
					e);
		}
		return new ArrayList<ByteArrayId>();
	}

	public CloseableIterator<?> query(
			final AccumuloFilteredIndexQuery query ) {
		return query.query(
				accumuloOperations,
				adapterStore,
				0);
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
				null,
				new UniformVisibilityWriter<T>(
						new UnconstrainedVisibilityHandler<T, Object>()));
	}

	@Override
	public <T> void ingest(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback ) {
		this.ingest(
				dataWriter,
				index,
				entryIterator,
				ingestCallback,
				new UniformVisibilityWriter<T>(
						new UnconstrainedVisibilityHandler<T, Object>()));
	}

	@Override
	public <T> void ingest(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
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
					ingestCallback,
					customFieldVisibilityWriter);
		}
		else {
			ingestInternal(
					dataWriter,
					index,
					entryIterator,
					ingestCallback,
					customFieldVisibilityWriter);
		}
	}

	private <T> void ingestInternal(
			final WritableDataAdapter<T> dataWriter,
			final Index index,
			final Iterator<T> entryIterator,
			final IngestCallback<T> ingestCallback,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
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
							((AttachedIteratorDataAdapter) dataWriter).getAttachedIteratorConfig(index));
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
			final StatsCompositionTool<T> statsCompositionTool = this.getStatsCompositionTool(dataWriter);
			callbacks.add(statsCompositionTool);

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
											entry,
											customFieldVisibilityWriter).iterator();
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
													entry,
													customFieldVisibilityWriter),
											entry);
								}
							});
				}
			});
			writer.close();

			if (useAltIndex && (altIdxWriter != null)) {
				altIdxWriter.close();
			}

			statsCompositionTool.flush();

		}
		catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to ingest data entries",
					e);
		}
	}

	protected static byte[] getRowIdBytes(
			final AccumuloRowId rowElements ) {
		final ByteBuffer buf = ByteBuffer.allocate(12 + rowElements.getDataId().length + rowElements.getAdapterId().length + rowElements.getInsertionId().length);
		buf.put(rowElements.getInsertionId());
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
			final ByteArrayId adapterId,
			final String... additionalAuthorizations ) {
		final String altIdxTableName = index.getId().getString() + AccumuloUtils.ALT_INDEX_TABLE;

		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(altIdxTableName)) {
			final List<ByteArrayId> rowIds = getAltIndexRowIds(
					altIdxTableName,
					dataId,
					adapterId,
					1);

			if (rowIds.size() > 0) {
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
					adapterId,
					1,
					additionalAuthorizations);
			final Entry<Key, Value> row = rows.size() > 0 ? rows.get(0) : null;

			if (row != null) {
				return (T) AccumuloUtils.decodeRow(
						row.getKey(),
						row.getValue(),
						adapterStore.getAdapter(adapterId),
						index);
			}
		}
		return null;
	}

	@Override
	public boolean deleteEntry(
			final Index index,
			final ByteArrayId dataId,
			final ByteArrayId adapterId,
			final String... authorizations ) {
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;
		final boolean useAltIndex = accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(altIdxTableName);
		@SuppressWarnings("unchecked")
		final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterStore.getAdapter(adapterId);

		final List<Entry<Key, Value>> rows = (useAltIndex) ? getEntryRowWithRowIds(
				tableName,
				getAltIndexRowIds(
						altIdxTableName,
						dataId,
						adapterId,
						Integer.MAX_VALUE),
				adapterId,
				authorizations) : getEntryRows(
				tableName,
				dataId,
				adapterId,
				Integer.MAX_VALUE,
				authorizations);

		final StatsCompositionTool<Object> statsCompositionTool = getStatsCompositionTool(adapter);
		final boolean success = (rows.size() > 0) && deleteRowsForSingleEntry(
				tableName,
				rows,
				createDecodingDeleteObserver(
						statsCompositionTool,
						adapter,
						index),
				authorizations);

		if (success) {
			statsCompositionTool.flush();
		}
		if (success && useAltIndex) {
			deleteAltIndexEntry(
					altIdxTableName,
					dataId,
					adapterId);
		}

		return success;

	}

	@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "i is part of loop condition")
	private List<Entry<Key, Value>> getEntryRows(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId,
			final int limit,
			final String... authorizations ) {

		final List<Entry<Key, Value>> resultList = new ArrayList<Entry<Key, Value>>();
		ScannerBase scanner = null;
		try {

			scanner = accumuloOperations.createScanner(
					tableName,
					authorizations);

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
			int i = 0;
			if (iterator.hasNext() && (i < limit)) { // FB supression as FB not
														// detecting i reference
														// here
				resultList.add(iterator.next());
				i++;
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}
		finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		return resultList;
	}

	private List<Entry<Key, Value>> getEntryRowWithRowIds(
			final String tableName,
			final List<ByteArrayId> rowIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {

		final List<Entry<Key, Value>> resultList = new ArrayList<Entry<Key, Value>>();
		if ((rowIds == null) || rowIds.isEmpty()) {
			return resultList;
		}
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		for (final ByteArrayId row : rowIds) {
			ranges.add(new ByteArrayRange(
					row,
					row));
		}
		ScannerBase scanner = null;
		try {
			scanner = accumuloOperations.createBatchScanner(
					tableName,
					authorizations);
			((BatchScanner) scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges(ranges));

			final IteratorSetting iteratorSettings = new IteratorSetting(
					QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
					QueryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
					WholeRowIterator.class);
			scanner.addScanIterator(iteratorSettings);

			final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
			while (iterator.hasNext()) {
				resultList.add(iterator.next());
			}
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}
		finally {
			if (scanner != null) {
				scanner.close();
			}
		}

		return resultList;
	}

	private List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final ByteArrayId dataId,
			final ByteArrayId adapterId,
			final int limit ) {

		final List<ByteArrayId> result = new ArrayList<ByteArrayId>();
		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(tableName)) {
			ScannerBase scanner = null;
			try {
				scanner = accumuloOperations.createScanner(tableName);

				((Scanner) scanner).setRange(Range.exact(new Text(
						dataId.getBytes())));

				scanner.fetchColumnFamily(new Text(
						adapterId.getBytes()));

				final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
				int i = 0;
				while (iterator.hasNext() && (i < limit)) {
					result.add(new ByteArrayId(
							iterator.next().getKey().getColumnQualifierData().getBackingArray()));
					i++;
				}

			}
			catch (final TableNotFoundException e) {
				LOGGER.warn(
						"Unable to query table '" + tableName + "'.  Table does not exist.",
						e);
			}
			finally {
				if (scanner != null) {
					scanner.close();
				}
			}
		}

		return result;
	}

	private boolean deleteAltIndexEntry(
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
			final ByteArrayId rowPrefix,
			final String... additionalAuthorizations ) {
		final AccumuloRowPrefixQuery q = new AccumuloRowPrefixQuery(
				index,
				rowPrefix,
				additionalAuthorizations);
		return q.query(
				accumuloOperations,
				adapterStore);
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
			final Index index,
			final Query query,
			final int limit,
			final String... authorizations ) {
		return query(
				adapter,
				index,
				query,				
				Integer.valueOf(limit),
				null,
				authorizations);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Query query,
			final int limit ) {
		return query(
				adapter,
				query,
				Integer.valueOf(limit));
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
				limit,
				null));
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
				limit,
				null);
	}

	@Override
	public CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query ) {
		return query(
				adapterIds,
				query,
				adapterStore,
				null,
				null);
	}

	private CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query,
			final AdapterStore adapterStore,
			final Integer limit,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		try (final CloseableIterator<Index> indices = indexStore.getIndices()) {
			return query(
					adapterIds,
					query,
					indices,
					adapterStore,
					limit,
					scanCallback,
					authorizations);
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

	private CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query,
			final CloseableIterator<Index> indices,
			final AdapterStore adapterStore,
			final Integer limit,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		// query the indices that are supported for this query object, and these
		// data adapter Ids
		final List<CloseableIterator<?>> results = new ArrayList<CloseableIterator<?>>();
		int indexCount = 0;
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final MultiIndexDedupeFilter clientDedupeFilter = new MultiIndexDedupeFilter();
		while (indices.hasNext()) {
			final Index index = indices.next();
			final AccumuloConstraintsQuery accumuloQuery;
			if (query == null) {
				accumuloQuery = new AccumuloConstraintsQuery(
						adapterIds,
						index,
						clientDedupeFilter,
						scanCallback,
						authorizations);
			}
			else if (query.isSupported(index)) {
				// construct the query
				accumuloQuery = new AccumuloConstraintsQuery(
						adapterIds,
						index,
						query.getIndexConstraints(index.getIndexStrategy()),
						query.createFilters(index.getIndexModel()),
						clientDedupeFilter,
						scanCallback,
						authorizations);
			}
			else {
				continue;
			}
			results.add(accumuloQuery.query(
					accumuloOperations,
					adapterStore,
					limit,
					true));
			indexCount++;
		}
		// if there aren't multiple indices, the client-side dedupe filter can
		// just cache rows that are duplicated within the index and not
		// everything
		clientDedupeFilter.setMultiIndexSupportEnabled(indexCount > 1);
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
		if ((query != null) && !query.isSupported(index)) {
			throw new IllegalArgumentException(
					"Index does not support the query");
		}
		return (CloseableIterator<T>) query(
				null,
				query,
				new CloseableIterator.Wrapper(
						Arrays.asList(
								new Index[] {
									index
								}).iterator()),
				adapterStore,
				limit,
				null);
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
				limit,
				(String[])null);
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
				null,
				null);
	}

	@SuppressWarnings("unchecked")
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final Integer limit,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		if ((query != null) && !query.isSupported(index)) {
			throw new IllegalArgumentException(
					"Index does not support the query");
		}
		store(adapter);

		return (CloseableIterator<T>) query(
				Arrays.asList(new ByteArrayId[] {
					adapter.getAdapterId()
				}),
				query,
				new CloseableIterator.Wrapper(
						Arrays.asList(
								new Index[] {
									index
								}).iterator()),
				new MemoryAdapterStore(
						new DataAdapter[] {
							adapter
						}),
				limit,
				scanCallback,
				authorizations);
	}

	public <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final Index index,
			final String... additionalAuthorizations ) {
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;
		final String adapterId = StringUtils.stringFromBinary(adapter.getAdapterId().getBytes());

		final CloseableIterator<DataStatistics<?>> it = statisticsStore.getDataStatistics(adapter.getAdapterId());

		while (it.hasNext()) {
			final DataStatistics stats = it.next();
			statisticsStore.removeStatistics(
					adapter.getAdapterId(),
					stats.getStatisticsId(),
					additionalAuthorizations);
		}

		deleteAll(
				tableName,
				adapterId,
				additionalAuthorizations);
		deleteAll(
				altIdxTableName,
				adapterId,
				additionalAuthorizations);
	}

	private <T> StatsCompositionTool<T> getStatsCompositionTool(
			final DataAdapter<T> adapter ) {
		return new StatsCompositionTool<T>(
				adapter,
				accumuloOptions.isPersistDataStatistics() ? statisticsStore : null);
	}

	private boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		BatchDeleter deleter = null;
		try {
			deleter = accumuloOperations.createBatchDeleter(
					tableName,
					additionalAuthorizations);

			deleter.setRanges(Arrays.asList(new Range()));
			deleter.fetchColumnFamily(new Text(
					columnFamily));
			deleter.delete();
			return true;
		}
		catch (final TableNotFoundException | MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to delete row from table [" + tableName + "].",
					e);
			return false;
		}
		finally {
			if (deleter != null) {
				deleter.close();
			}
		}

	}

	/**
	 * Delete rows associated with a single entry
	 * 
	 * @param tableName
	 * @param rows
	 * @param deleteRowObserver
	 * @param authorizations
	 * @return
	 */
	private boolean deleteRowsForSingleEntry(
			final String tableName,
			final List<Entry<Key, Value>> rows,
			final DeleteRowObserver deleteRowObserver,
			final String... authorizations ) {

		BatchDeleter deleter = null;
		try {
			deleter = accumuloOperations.createBatchDeleter(
					tableName,
					authorizations);
			int count = 0;
			final List<Range> rowRanges = new ArrayList<Range>();
			for (final Entry<Key, Value> rowData : rows) {
				final byte[] id = rowData.getKey().getRowData().getBackingArray();
				rowRanges.add(Range.exact(new Text(
						id)));
				if (deleteRowObserver != null) {
					deleteRowObserver.deleteRow(
							rowData.getKey(),
							rowData.getValue());
				}
				count++;
			}
			deleter.setRanges(rowRanges);

			deleter.delete();

			deleter.close();

			return count > 0;
		}
		catch (final TableNotFoundException | MutationsRejectedException e) {
			LOGGER.warn(
					"Unable to delete row from table [" + tableName + "].",
					e);
			if (deleter != null) {
				deleter.close();
			}
			return false;
		}

	}

	private DeleteRowObserver createDecodingDeleteObserver(
			final StatsCompositionTool<Object> stats,
			final DataAdapter<Object> adapter,
			final Index index ) {

		return stats.isPersisting() ?  new DeleteRowObserver() {
			// many rows can be associated with one entry.
			// need a control to delete only one.
			boolean foundOne = false;

			@Override
			public void deleteRow(
					final Key key,
					final Value value ) {
				if (!foundOne) {
					final AccumuloRowId rowId = new AccumuloRowId(
							key.getRow().copyBytes());
					AccumuloUtils.decodeRow(
							key,
							value,
							rowId,
							adapter,
							null,
							null,
							index,
							new ScanCallback<Object>() {

								@Override
								public void entryScanned(
										final DataStoreEntryInfo entryInfo,
										final Object entry ) {
									stats.entryDeleted(
											entryInfo,
											entry);
								}

							});

				}
				foundOne = true;
			}
		} : null;
	}

	private interface DeleteRowObserver
	{
		public void deleteRow(
				Key key,
				Value value );
	}
}
