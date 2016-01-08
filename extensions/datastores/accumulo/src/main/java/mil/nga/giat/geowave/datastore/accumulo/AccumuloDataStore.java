package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreCallbackManager;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.AdapterIdQuery;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloMRUtils;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveAccumuloRecordReader;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowPrefixQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.SingleEntryFilterIterator;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.EntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.ScannerClosableWrapper;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
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
		MapReduceDataStore
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStore.class);

	protected final IndexStore indexStore;
	protected final AdapterStore adapterStore;
	protected final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
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
				new AccumuloSecondaryIndexDataStore(
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
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations),
				accumuloOperations,
				accumuloOptions);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final AccumuloOperations accumuloOperations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				secondaryIndexDataStore,
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		this.secondaryIndexDataStore = secondaryIndexDataStore;
	}

	@Override
	public <T> IndexWriter createIndexWriter(
			final PrimaryIndex index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		return new AccumuloIndexWriter(
				index,
				accumuloOperations,
				accumuloOptions,
				this,
				statisticsStore,
				secondaryIndexDataStore,
				customFieldVisibilityWriter);
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (accumuloOptions.isPersistAdapter() && !adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
	}

	protected synchronized void store(
			final PrimaryIndex index ) {
		if (accumuloOptions.isPersistIndex() && !indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}

	/*
	 * Since this general-purpose method crosses multiple adapters, the type of
	 * result cannot be assumed.
	 * 
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.DataStore#query(mil.nga.giat.geowave.
	 * core.store.query.QueryOptions,
	 * mil.nga.giat.geowave.core.store.query.Query)
	 */
	@Override
	public <T> CloseableIterator<T> query(
			final QueryOptions queryOptions,
			final Query query ) {
		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final QueryOptions sanitizedQueryOptions = (queryOptions == null) ? new QueryOptions() : queryOptions;
		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		int indexCount = 0;
		final DedupeFilter filter = new DedupeFilter();
		MemoryAdapterStore tempAdapterStore;
		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));

			try (CloseableIterator<Index<?, ?>> indexIt = sanitizedQueryOptions.getIndices(indexStore)) {
				while (indexIt.hasNext()) {
					final PrimaryIndex index = (PrimaryIndex) indexIt.next();
					indexCount++;
					if (sanitizedQuery instanceof RowIdQuery) {
						final AccumuloRowIdsQuery<Object> q = new AccumuloRowIdsQuery<Object>(
								sanitizedQueryOptions.getAdapterIds(adapterStore),
								index,
								((RowIdQuery) sanitizedQuery).getRowIds(),
								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
								filter,
								sanitizedQueryOptions.getAuthorizations());

						results.add(q.query(
								accumuloOperations,
								tempAdapterStore,
								-1));
						continue;
					}
					else if (sanitizedQuery instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) sanitizedQuery;
						results.add(getEntries(
								index,
								idQuery.getDataIds(),
								(DataAdapter<Object>) adapterStore.getAdapter(idQuery.getAdapterId()),
								filter,
								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
								sanitizedQueryOptions.getAuthorizations(),
								true));
						continue;
					}
					else if (sanitizedQuery instanceof PrefixIdQuery) {
						final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
						final AccumuloRowPrefixQuery<Object> prefixQuery = new AccumuloRowPrefixQuery<Object>(
								index,
								prefixIdQuery.getRowPrefix(),
								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
								sanitizedQueryOptions.getLimit(),
								sanitizedQueryOptions.getAuthorizations());
						results.add(prefixQuery.query(
								accumuloOperations,
								tempAdapterStore));
						continue;
					}
					else if (sanitizedQuery instanceof AdapterIdQuery) {
						final AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
								Collections.singletonList(((AdapterIdQuery) sanitizedQuery).getAdapterId()),
								index,
								sanitizedQuery,
								filter,
								sanitizedQueryOptions.getScanCallback(),
								sanitizedQueryOptions.getAuthorizations());
						results.add(accumuloQuery.query(
								accumuloOperations,
								tempAdapterStore,
								sanitizedQueryOptions.getLimit(),
								true));
						continue;

					}

					AccumuloConstraintsQuery accumuloQuery;
					try {
						List<ByteArrayId> adapterIds = sanitizedQueryOptions.getAdapterIds(adapterStore);
						// only narrow adapter Ids if the set of adapter id's is
						// resolved
						try (CloseableIterator<DataAdapter<?>> adapters = sanitizedQueryOptions.getAdapters(getAdapterStore())) {
							adapterIds = ((adapterIds != null) && accumuloOptions.persistDataStatistics && (adapterIds.size() > 0)) ? DataStoreUtils.trimAdapterIdsByIndex(
									statisticsStore,
									index.getId(),
									adapters,
									sanitizedQueryOptions.getAuthorizations()) : adapterIds;
						}
						// the null case should not happen, but the findbugs
						// seems to like it.
						if ((adapterIds == null) || (adapterIds.size() > 0)) {
							accumuloQuery = new AccumuloConstraintsQuery(
									adapterIds,
									index,
									sanitizedQuery,
									filter,
									sanitizedQueryOptions.getScanCallback(),
									sanitizedQueryOptions.getAuthorizations());

							results.add(accumuloQuery.query(
									accumuloOperations,
									tempAdapterStore,
									sanitizedQueryOptions.getLimit(),
									true));
						}
					}
					catch (final IOException e) {
						LOGGER.error("Cannot resolve adapter Ids " + sanitizedQueryOptions.toString());

					}
				}
			}

		}
		catch (final IOException e1) {
			LOGGER.error(
					"Failed to resolve adapter or index for query",
					e1);
		}

		if (sanitizedQueryOptions.isDedupAcrossIndices() && (indexCount > 1)) {
			filter.setDedupAcrossIndices(true);
		}

		return new CloseableIteratorWrapper<T>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<Object> result : results) {
							result.close();
						}
					}
				},
				Iterators.concat(new CastIterator<T>(
						results.iterator())));
	}

	protected static class CastIterator<T> implements
			Iterator<CloseableIterator<T>>
	{

		final Iterator<CloseableIterator<Object>> it;

		public CastIterator(
				final Iterator<CloseableIterator<Object>> it ) {
			this.it = it;
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public CloseableIterator<T> next() {
			return (CloseableIterator<T>) it.next();
		}

		@Override
		public void remove() {
			it.remove();
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

	@SuppressWarnings("unchecked")
	private CloseableIterator<Object> getEntries(
			final PrimaryIndex index,
			final List<ByteArrayId> dataIds,
			final DataAdapter<Object> adapter,
			final DedupeFilter dedupeFilter,
			final ScanCallback<Object> callback,
			final String[] authorizations,
			final boolean limit )
			throws IOException {
		final String altIdxTableName = index.getId().getString() + AccumuloUtils.ALT_INDEX_TABLE;

		MemoryAdapterStore tempAdapterStore;

		tempAdapterStore = new MemoryAdapterStore(
				new DataAdapter[] {
					adapter
				});

		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(altIdxTableName)) {
			final List<ByteArrayId> rowIds = getAltIndexRowIds(
					altIdxTableName,
					dataIds,
					adapter.getAdapterId(),
					limit ? 1 : -1);

			if (rowIds.size() > 0) {
				final AccumuloRowIdsQuery<Object> q = new AccumuloRowIdsQuery<Object>(
						adapter,
						index,
						rowIds,
						callback,
						dedupeFilter,
						authorizations);

				return q.query(
						accumuloOperations,
						tempAdapterStore,
						(limit || (rowIds.size() < 2)) ? 1 : -1);
			}
		}
		else {
			return getEntryRows(
					index,
					tempAdapterStore,
					dataIds,
					adapter.getAdapterId(),
					callback,
					authorizations,
					limit ? 1 : -1);
		}
		return new CloseableIterator.Empty();
	}

	@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "i is part of loop condition")
	private CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final ScanCallback<Object> scanCallback,
			final String[] authorizations,
			final int limit ) {

		try {

			final ScannerBase scanner = accumuloOperations.createScanner(
					index.getId().getString(),
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
					SingleEntryFilterIterator.DATA_IDS,
					SingleEntryFilterIterator.encodeIDs(dataIds));
			scanner.addScanIterator(filterIteratorSettings);

			if (limit > 0) {
				((Scanner) scanner).setBatchSize(limit);
			}

			return new CloseableIteratorWrapper<Object>(
					new ScannerClosableWrapper(
							scanner),
					new EntryIteratorWrapper(
							adapterStore,
							index,
							scanner.iterator(),
							null,
							scanCallback));

		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + index.getId().getString() + "'.  Table does not exist.",
					e);
		}

		return null;
	}

	/*
	 * Perhaps a use for this optimization with DataIdQuery ?
	 * 
	 * private List<Entry<Key, Value>> getEntryRowWithRowIds( final String
	 * tableName, final List<ByteArrayId> rowIds, final ByteArrayId adapterId,
	 * final String... authorizations ) {
	 * 
	 * final List<Entry<Key, Value>> resultList = new ArrayList<Entry<Key,
	 * Value>>(); if ((rowIds == null) || rowIds.isEmpty()) { return resultList;
	 * } final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
	 * for (final ByteArrayId row : rowIds) { ranges.add(new ByteArrayRange(
	 * row, row)); } ScannerBase scanner = null; try { scanner =
	 * accumuloOperations.createBatchScanner( tableName, authorizations);
	 * ((BatchScanner)
	 * scanner).setRanges(AccumuloUtils.byteArrayRangesToAccumuloRanges
	 * (ranges));
	 * 
	 * final IteratorSetting iteratorSettings = new IteratorSetting(
	 * QueryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
	 * QueryFilterIterator.WHOLE_ROW_ITERATOR_NAME, WholeRowIterator.class);
	 * scanner.addScanIterator(iteratorSettings);
	 * 
	 * final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
	 * while (iterator.hasNext()) { resultList.add(iterator.next()); } } catch
	 * (final TableNotFoundException e) { LOGGER.warn( "Unable to query table '"
	 * + tableName + "'.  Table does not exist.", e); } finally { if (scanner !=
	 * null) { scanner.close(); } }
	 * 
	 * return resultList; }
	 */

	private List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final int limit ) {

		final List<ByteArrayId> result = new ArrayList<ByteArrayId>();
		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(tableName)) {
			ScannerBase scanner = null;
			for (final ByteArrayId dataId : dataIds) {
				try {
					scanner = accumuloOperations.createScanner(tableName);

					((Scanner) scanner).setRange(Range.exact(new Text(
							dataId.getBytes())));

					scanner.fetchColumnFamily(new Text(
							adapterId.getBytes()));

					final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
					int i = 0;
					while (iterator.hasNext() && ((limit < 0) || (i < limit))) {
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
		}

		return result;
	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdaptersAndIndices()) {
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
				LOGGER.error(
						"Unable to delete all tables",
						e);
				return false;
			}
		}
		else if (query instanceof AdapterIdQuery) {
			try (CloseableIterator<Index<?, ?>> indexIt = queryOptions.getIndices(indexStore)) {
				while (indexIt.hasNext()) {
					deleteEntries(
							adapterStore.getAdapter(((AdapterIdQuery) query).getAdapterId()),
							indexIt.next(),
							queryOptions.getAuthorizations());
				}
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to delete all tables",
						e);
				return false;
			}
		}
		else {

			try (CloseableIterator<Index<?, ?>> indexIt = queryOptions.getIndices(indexStore)) {
				final AtomicBoolean aOk = new AtomicBoolean(
						true);
				while (indexIt.hasNext() && aOk.get()) {
					final PrimaryIndex index = (PrimaryIndex) indexIt.next();
					final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
					final String altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;
					final boolean useAltIndex = accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(altIdxTableName);
					final BatchDeleter idxDeleter = accumuloOperations.createBatchDeleter(
							tableName,
							queryOptions.getAuthorizations());
					final BatchDeleter altIdxDelete = useAltIndex ? accumuloOperations.createBatchDeleter(
							altIdxTableName,
							queryOptions.getAuthorizations()) : null;

					try (final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore)) {
						callbackCache.setPersistStats(accumuloOptions.persistDataStatistics);

						try (final CloseableIterator<DataAdapter<?>> adapterIt = queryOptions.getAdapters(adapterStore)) {
							while (adapterIt.hasNext()) {
								final DataAdapter<Object> adapter = (DataAdapter<Object>) adapterIt.next();

								final ScanCallback<Object> callback = new ScanCallback<Object>() {
									@Override
									public void entryScanned(
											final DataStoreEntryInfo entryInfo,
											final Object entry ) {
										callbackCache.getDeleteCallback(
												(WritableDataAdapter<Object>) adapter,
												index).entryDeleted(
												entryInfo,
												entry);
										try {
											addToBatch(
													idxDeleter,
													entryInfo.getRowIds());
											if (useAltIndex) {
												addToBatch(
														altIdxDelete,
														Collections.singletonList(adapter.getDataId(entry)));
											}
										}
										catch (final MutationsRejectedException e) {
											LOGGER.error(
													"Failed deletion",
													e);
											aOk.set(false);
										}
										catch (final TableNotFoundException e) {
											LOGGER.error(
													"Failed deletion",
													e);
											aOk.set(false);
										}

									}
								};

								CloseableIterator<?> dataIt = null;
								if (query instanceof RowIdQuery) {
									final AccumuloRowIdsQuery<Object> q = new AccumuloRowIdsQuery<Object>(
											queryOptions.getAdapterIds(adapterStore),
											index,
											((RowIdQuery) query).getRowIds(),
											callback,
											null,
											queryOptions.getAuthorizations());

									dataIt = q.query(
											accumuloOperations,
											adapterStore,
											-1);
								}
								else if (query instanceof DataIdQuery) {
									final DataIdQuery idQuery = (DataIdQuery) query;
									dataIt = getEntries(
											index,
											idQuery.getDataIds(),
											(DataAdapter<Object>) adapterStore.getAdapter(idQuery.getAdapterId()),
											null,
											callback,
											queryOptions.getAuthorizations(),
											false);
								}
								else if (query instanceof PrefixIdQuery) {
									dataIt = new AccumuloRowPrefixQuery<Object>(
											index,
											((PrefixIdQuery) query).getRowPrefix(),
											callback,
											null,
											queryOptions.getAuthorizations()).query(
											accumuloOperations,
											adapterStore);

								}
								else {
									dataIt = new AccumuloConstraintsQuery(
											Collections.singletonList(adapter.getAdapterId()),
											index,
											query,
											null,
											callback,
											queryOptions.getAuthorizations()).query(
											accumuloOperations,
											adapterStore,
											null);
								}

								while (dataIt.hasNext()) {
									dataIt.next();
								}
								try {
									dataIt.close();
								}
								catch (final Exception ex) {
									LOGGER.warn(
											"Cannot close iterator",
											ex);
								}
							}
						}
					}
					idxDeleter.close();
					if (altIdxDelete != null) {
						altIdxDelete.close();
					}
				}
				return aOk.get();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Failed delete operation " + query.toString(),
						e);
				return false;
			}
			catch (final TableNotFoundException e1) {
				LOGGER.error(
						"Failed delete operation " + query.toString(),
						e1);
				return false;
			}
		}

		return true;

	}

	private <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final Index index,
			final String... additionalAuthorizations )
			throws IOException {
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;
		final String adapterId = StringUtils.stringFromBinary(adapter.getAdapterId().getBytes());

		try (final CloseableIterator<DataStatistics<?>> it = statisticsStore.getDataStatistics(adapter.getAdapterId())) {

			while (it.hasNext()) {
				final DataStatistics stats = it.next();
				statisticsStore.removeStatistics(
						adapter.getAdapterId(),
						stats.getStatisticsId(),
						additionalAuthorizations);
			}
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

	private void addToBatch(
			final BatchDeleter deleter,
			final List<ByteArrayId> ids )
			throws MutationsRejectedException,
			TableNotFoundException {
		final List<Range> rowRanges = new ArrayList<Range>();
		for (final ByteArrayId id : ids) {
			rowRanges.add(Range.exact(new Text(
					id.getBytes())));
		}
		deleter.setRanges(rowRanges);
		deleter.delete();
	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		return AccumuloMRUtils.getSplits(
				accumuloOperations,
				query,
				queryOptions,
				adapterStore,
				statsStore,
				indexStore,
				minSplits,
				maxSplits);
	}

	@Override
	public RecordReader<GeoWaveInputKey, ?> createRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final boolean isOutputWritable,
			final InputSplit inputSplit )
			throws IOException,
			InterruptedException {
		return new GeoWaveAccumuloRecordReader(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore,
				accumuloOperations);
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}

	public DataStatisticsStore getStatisticsStore() {
		return statisticsStore;
	}

	public SecondaryIndexDataStore getSecondaryIndexDataStore() {
		return secondaryIndexDataStore;
	}

}
