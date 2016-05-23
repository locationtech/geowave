package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CastIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.core.store.DataStoreCallbackManager;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndependentAdapterIndexWriter;
import mil.nga.giat.geowave.core.store.IndexCompositeWriter;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.IngestCallbackList;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
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
import mil.nga.giat.geowave.datastore.accumulo.metadata.AbstractAccumuloPersistence;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowPrefixQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.SingleEntryFilterIterator;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.EntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.ScannerClosableWrapper;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

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
	protected final AdapterIndexMappingStore indexMappingStore;

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
				new AccumuloAdapterIndexMappingStore(
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
						accumuloOperations,
						accumuloOptions),
				new AccumuloAdapterIndexMappingStore(
						accumuloOperations),
				accumuloOperations,
				accumuloOptions);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				secondaryIndexDataStore,
				indexMappingStore,
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		this.secondaryIndexDataStore = secondaryIndexDataStore;
		this.indexMappingStore = indexMappingStore;
	}

	@Override
	public <T> IndexWriter<T> createWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex... indices )
			throws MismatchedIndexToAdapterMapping {
		store(adapter);

		indexMappingStore.addAdapterIndexMapping(new AdapterToIndexMapping(
				adapter.getAdapterId(),
				indices));

		final byte[] adapterId = adapter.getAdapterId().getBytes();
		final IndexWriter<T>[] writers = new IndexWriter[indices.length];

		int i = 0;
		for (final PrimaryIndex index : indices) {
			final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
					statisticsStore,
					secondaryIndexDataStore,
					i == 0);

			callbackManager.setPersistStats(accumuloOptions.isPersistDataStatistics());

			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();

			store(index);

			final String indexName = index.getId().getString();

			if (adapter instanceof WritableDataAdapter) {
				if (accumuloOptions.isUseAltIndex()) {
					try {
						callbacks.add(new AltIndexCallback<T>(
								indexName,
								(WritableDataAdapter<T>) adapter,
								accumuloOptions));

					}
					catch (final TableNotFoundException e) {
						LOGGER.error(
								"Unable to create table table for alt index to  [" + index.getId().getString() + "]",
								e);
					}
				}
			}
			callbacks.add(callbackManager.getIngestCallback(
					(WritableDataAdapter<T>) adapter,
					index));

			try {
				if (adapter instanceof RowMergingDataAdapter) {
					if (!DataAdapterAndIndexCache.getInstance(
							RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID).add(
							adapter.getAdapterId(),
							indexName)) {
						AccumuloUtils.attachRowMergingIterators(
								((RowMergingDataAdapter<?, ?>) adapter),
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
						"Unable to determine existence of locality group [" + adapter.getAdapterId().getString() + "]",
						e);
			}

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<T>(
					callbacks);
			writers[i] = new AccumuloIndexWriter(
					adapter,
					index,
					accumuloOperations,
					accumuloOptions,
					callbacksList,
					callbacksList,
					DataStoreUtils.UNCONSTRAINED_VISIBILITY);

			if (adapter instanceof IndexDependentDataAdapter) {
				writers[i] = new IndependentAdapterIndexWriter<T>(
						(IndexDependentDataAdapter<T>) adapter,
						index,
						writers[i]);
			}
			i++;
		}
		return new IndexCompositeWriter(
				writers);

	}

	private class AltIndexCallback<T> implements
			IngestCallback<T>,
			Closeable,
			Flushable
	{

		private final WritableDataAdapter<T> adapter;
		private Writer altIdxWriter;
		private final String altIdxTableName;

		public AltIndexCallback(
				final String indexName,
				final WritableDataAdapter<T> adapter,
				final AccumuloOptions accumuloOptions )
				throws TableNotFoundException {
			this.adapter = adapter;
			altIdxTableName = indexName + AccumuloUtils.ALT_INDEX_TABLE;
			if (accumuloOperations.tableExists(indexName)) {
				if (!accumuloOperations.tableExists(altIdxTableName)) {
					throw new TableNotFoundException(
							altIdxTableName,
							altIdxTableName,
							"Requested alternate index table does not exist.");
				}
			}
			else {
				// index table does not exist yet
				if (accumuloOperations.tableExists(altIdxTableName)) {
					accumuloOperations.deleteTable(altIdxTableName);
					LOGGER.warn("Deleting current alternate index table [" + altIdxTableName
							+ "] as main table does not yet exist.");
				}
			}

			altIdxWriter = accumuloOperations.createWriter(
					altIdxTableName,
					accumuloOptions.isCreateTable(),
					true,
					accumuloOptions.isEnableBlockCache(),
					null);
		}

		@Override
		public void close()
				throws IOException {
			altIdxWriter.close();
			altIdxWriter = null;
		}

		@Override
		public void entryIngested(
				final DataStoreEntryInfo entryInfo,
				final T entry ) {
			AccumuloUtils.writeAltIndex(
					adapter,
					entryInfo,
					entry,
					altIdxWriter);

		}

		@Override
		public void flush() {
			altIdxWriter.flush();
		}

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

		final DedupeFilter filter = new DedupeFilter();
		MemoryAdapterStore tempAdapterStore;
		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));

			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : sanitizedQueryOptions
					.getAdaptersWithMinimalSetOfIndices(
							tempAdapterStore,
							indexMappingStore,
							indexStore)) {
				final List<ByteArrayId> adapterIdsToQuery = new ArrayList<>();
				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {
					if (sanitizedQuery instanceof RowIdQuery) {
						final AccumuloRowIdsQuery<Object> q = new AccumuloRowIdsQuery<Object>(
								adapter,
								indexAdapterPair.getLeft(),
								((RowIdQuery) sanitizedQuery).getRowIds(),
								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
								filter,
								sanitizedQueryOptions.getAuthorizations());

						results.add(q.query(
								accumuloOperations,
								tempAdapterStore,
								sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
								-1));
						continue;
					}
					else if (sanitizedQuery instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) sanitizedQuery;
						if (idQuery.getAdapterId().equals(
								adapter.getAdapterId())) {
							results.add(getEntries(
									indexAdapterPair.getLeft(),
									idQuery.getDataIds(),
									(DataAdapter<Object>) adapterStore.getAdapter(idQuery.getAdapterId()),
									filter,
									(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
									sanitizedQueryOptions.getAuthorizations(),
									sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
									true));
						}
						continue;
					}
					else if (sanitizedQuery instanceof PrefixIdQuery) {
						final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
						final AccumuloRowPrefixQuery<Object> prefixQuery = new AccumuloRowPrefixQuery<Object>(
								indexAdapterPair.getLeft(),
								prefixIdQuery.getRowPrefix(),
								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
								sanitizedQueryOptions.getLimit(),
								sanitizedQueryOptions.getAuthorizations());
						results.add(prefixQuery.query(
								accumuloOperations,
								sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
								tempAdapterStore));
						continue;
					}
					adapterIdsToQuery.add(adapter.getAdapterId());
				}
				// supports querying multiple adapters in a single index
				// in one query instance (one scanner) for efficiency
				if (adapterIdsToQuery.size() > 0) {
					AccumuloConstraintsQuery accumuloQuery;

					accumuloQuery = new AccumuloConstraintsQuery(
							adapterIdsToQuery,
							indexAdapterPair.getLeft(),
							sanitizedQuery,
							filter,
							sanitizedQueryOptions.getScanCallback(),
							sanitizedQueryOptions.getAggregation(),
							sanitizedQueryOptions.getFieldIdsAdapterPair(),
							composeMetaData(
									indexAdapterPair.getLeft(),
									adapterIdsToQuery,
									sanitizedQueryOptions.getAuthorizations()),
							sanitizedQueryOptions.getAuthorizations());

					results.add(accumuloQuery.query(
							accumuloOperations,
							tempAdapterStore,
							sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
							sanitizedQueryOptions.getLimit()));
				}
			}

		}
		catch (final IOException e1)

		{
			LOGGER.error(
					"Failed to resolve adapter or index for query",
					e1);
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

	protected static byte[] getRowIdBytes(
			final AccumuloRowId rowElements ) {
		final ByteBuffer buf = ByteBuffer.allocate(12 + rowElements.getDataId().length
				+ rowElements.getAdapterId().length + rowElements.getInsertionId().length);
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
			final double[] maxResolutionSubsamplingPerDimension,
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
						maxResolutionSubsamplingPerDimension,
						(limit || (rowIds.size() < 2)) ? 1 : -1);
			}
		}
		else {
			return getEntryRows(
					index,
					tempAdapterStore,
					dataIds,
					adapter,
					callback,
					dedupeFilter,
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
			final DataAdapter<?> adapter,
			final ScanCallback<Object> scanCallback,
			final DedupeFilter dedupeFilter,
			final String[] authorizations,
			final int limit ) {

		try {

			final ScannerBase scanner = accumuloOperations.createScanner(
					index.getId().getString(),
					authorizations);

			scanner.fetchColumnFamily(new Text(
					adapter.getAdapterId().getBytes()));

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
					ByteArrayUtils.byteArrayToString(adapter.getAdapterId().getBytes()));

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
							dedupeFilter,
							scanCallback));

		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + index.getId().getString() + "'.  Table does not exist.",
					e);
		}

		return null;
	}

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
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			try {

				// TODO These interfaces should all provide remove and removeAll
				// capabilities instead of having to clear the
				// AbstractPersistence's cache manually
				((AbstractAccumuloPersistence) indexStore).clearCache();
				((AbstractAccumuloPersistence) adapterStore).clearCache();
				((AbstractAccumuloPersistence) statisticsStore).clearCache();
				((AccumuloSecondaryIndexDataStore) secondaryIndexDataStore).clearCache();
				((AbstractAccumuloPersistence) indexMappingStore).clearCache();

				accumuloOperations.deleteAll();
				return true;
			}
			catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
				LOGGER.error(
						"Unable to delete all tables",
						e);

			}
			return false;
		}

		final AtomicBoolean aOk = new AtomicBoolean(
				true);

		// keep a list of adapters that have been queried, to only low an
		// adapter to be queried
		// once
		final Set<ByteArrayId> queriedAdapters = new HashSet<ByteArrayId>();

		try {
			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : queryOptions
					.getIndicesForAdapters(
							adapterStore,
							indexMappingStore,
							indexStore)) {
				final PrimaryIndex index = indexAdapterPair.getLeft();
				if (index == null) {
					continue;
				}
				final String indexTableName = index.getId().getString();
				final String altIdxTableName = indexTableName + AccumuloUtils.ALT_INDEX_TABLE;

				final BatchDeleter idxDeleter = accumuloOperations.createBatchDeleter(
						indexTableName,
						queryOptions.getAuthorizations());

				final BatchDeleter altIdxDelete = accumuloOptions.isUseAltIndex()
						&& accumuloOperations.tableExists(altIdxTableName) ? accumuloOperations.createBatchDeleter(
						altIdxTableName,
						queryOptions.getAuthorizations()) : null;

				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

					final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore,
							queriedAdapters.add(adapter.getAdapterId()));

					callbackCache.setPersistStats(accumuloOptions.isPersistDataStatistics());

					if (query instanceof EverythingQuery) {
						deleteEntries(
								adapter,
								index,
								queryOptions.getAuthorizations());
						continue;
					}

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
								if (altIdxDelete != null) {
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
								adapter,
								index,
								((RowIdQuery) query).getRowIds(),
								callback,
								null,
								queryOptions.getAuthorizations());

						dataIt = q.query(
								accumuloOperations,
								adapterStore,
								null,
								-1);
					}
					else if (query instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) query;
						dataIt = getEntries(
								index,
								idQuery.getDataIds(),
								adapter,
								null,
								callback,
								queryOptions.getAuthorizations(),
								null,
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
								null,
								adapterStore);

					}
					else {
						List<ByteArrayId> adapterIds = Collections.singletonList(adapter.getAdapterId());
						dataIt = new AccumuloConstraintsQuery(
								adapterIds,
								index,
								query,
								null,
								callback,
								null,
								queryOptions.getFieldIdsAdapterPair(),
								composeMetaData(
										indexAdapterPair.getLeft(),
										adapterIds,
										queryOptions.getAuthorizations()),
								queryOptions.getAuthorizations()).query(
								accumuloOperations,
								adapterStore,
								null,
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
					callbackCache.close();
				}
				if (altIdxDelete != null) {
					altIdxDelete.close();
				}
				idxDeleter.close();
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

	private <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
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

		// cannot delete because authorizations are not used
		// this.indexMappingStore.remove(adapter.getAdapterId());

		deleteAll(
				tableName,
				adapterId,
				additionalAuthorizations);
		if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(altIdxTableName)) {
			deleteAll(
					altIdxTableName,
					adapterId,
					additionalAuthorizations);
		}
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
				indexMappingStore,
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

	private IndexMetaDataSet composeMetaData(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIdsToQuery,
			final String... authorizations ) {
		IndexMetaDataSet metaData = new IndexMetaDataSet(
				index.getId(),
				index.getId(),
				index.getIndexStrategy().createMetaData());
		for (ByteArrayId adapterId : adapterIdsToQuery) {
			metaData.merge((IndexMetaDataSet) statisticsStore.getDataStatistics(
					adapterId,
					IndexMetaDataSet.composeId(index.getId()),
					authorizations));
		}
		return metaData;
	}
}
