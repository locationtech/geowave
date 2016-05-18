/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CastIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
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
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
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
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseRecordReader;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.HBaseMRUtils;
import mil.nga.giat.geowave.datastore.hbase.metadata.AbstractHBasePersistence;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseConstraintsQuery;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseRowIdsQuery;
import mil.nga.giat.geowave.datastore.hbase.query.SingleEntryFilter;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCloseableIteratorWrapper.MultiScannerClosableWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class HBaseDataStore implements
		MapReduceDataStore
{

	private final static Logger LOGGER = Logger.getLogger(HBaseDataStore.class);
	private final IndexStore indexStore;
	private final AdapterStore adapterStore;
	private final BasicHBaseOperations operations;
	private final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
	protected final HBaseOptions options;
	protected final AdapterIndexMappingStore indexMappingStore;

	public HBaseDataStore(
			final BasicHBaseOperations operations ) {
		this(
				new HBaseIndexStore(
						operations),
				new HBaseAdapterStore(
						operations),
				new HBaseDataStatisticsStore(
						operations),
				new HBaseAdapterIndexMappingStore(
						operations),
				operations);
	}

	public HBaseDataStore(
			final BasicHBaseOperations operations,
			final HBaseOptions options ) {
		this(
				new HBaseIndexStore(
						operations),
				new HBaseAdapterStore(
						operations),
				new HBaseDataStatisticsStore(
						operations),
				new HBaseAdapterIndexMappingStore(
						operations),
				new HBaseSecondaryIndexDataStore(
						operations,
						options),
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final BasicHBaseOperations operations ) {
		// TODO Fix all constructor calls to pass secondary index store and get
		// rid of this constructor
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				null,
				operations,
				new HBaseOptions());
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final BasicHBaseOperations operations,
			final HBaseOptions options ) {
		// TODO Fix all constructor calls to pass secondary index store and get
		// rid of this constructor
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				null,
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final BasicHBaseOperations operations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				new HBaseOptions());
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final BasicHBaseOperations operations,
			final HBaseOptions options ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.indexMappingStore = indexMappingStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;
		this.operations = operations;
		this.options = options;
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

		final IndexWriter<T>[] writers = new IndexWriter[indices.length];

		int i = 0;
		for (final PrimaryIndex index : indices) {
			final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
					statisticsStore,
					secondaryIndexDataStore,
					i == 0);

			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();

			store(index);

			final String indexName = index.getId().getString();

			if (adapter instanceof WritableDataAdapter) {
				if (options.isUseAltIndex()) {
					try {
						callbacks.add(new AltIndexCallback<T>(
								indexName,
								(WritableDataAdapter<T>) adapter,
								options));

					}
					catch (final IOException e) {
						LOGGER.error(
								"Unable to create table table for alt index to  [" + index.getId().getString() + "]",
								e);
					}
				}
			}
			callbacks.add(callbackManager.getIngestCallback(
					(WritableDataAdapter<T>) adapter,
					index));

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<T>(
					callbacks);
			writers[i] = new HBaseIndexWriter(
					adapter,
					index,
					operations,
					options,
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
		final String altIdxTableName = index.getId().getString() + HBaseUtils.ALT_INDEX_TABLE;

		MemoryAdapterStore tempAdapterStore;

		tempAdapterStore = new MemoryAdapterStore(
				new DataAdapter[] {
					adapter
				});

		if (options.isUseAltIndex() && operations.tableExists(altIdxTableName)) {
			final List<ByteArrayId> rowIds = getAltIndexRowIds(
					altIdxTableName,
					dataIds,
					adapter.getAdapterId(),
					limit ? 1 : -1);

			if (rowIds.size() > 0) {
				final HBaseRowIdsQuery<Object> q = new HBaseRowIdsQuery<Object>(
						adapter,
						index,
						rowIds,
						callback,
						dedupeFilter,
						authorizations);

				return q.query(
						operations,
						tempAdapterStore,
						// TODO support this?
						// maxResolutionSubsamplingPerDimension,
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

	private CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final ScanCallback<Object> scanCallback,
			final String[] authorizations,
			final int limit ) {

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());

		final List<Iterator<Result>> resultList = new ArrayList<Iterator<Result>>();
		final List<ResultScanner> resultScanners = new ArrayList<ResultScanner>();
		Iterator<Result> iterator = null;

		try {

			for (final ByteArrayId dataId : dataIds) {
				final Scan scanner = new Scan();
				scanner.setFilter(new SingleEntryFilter(
						dataId.getBytes(),
						adapterId.getBytes()));
				final ResultScanner results = operations.getScannedResults(
						scanner,
						tableName);
				resultScanners.add(results);
				final Iterator<Result> resultIt = results.iterator();
				if (resultIt.hasNext()) {
					resultList.add(resultIt);
				}
			}

			iterator = Iterators.concat(resultList.iterator());
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}

		return new HBaseCloseableIteratorWrapper<Object>(
				new MultiScannerClosableWrapper(
						resultScanners),
				new HBaseEntryIteratorWrapper(
						adapterStore,
						index,
						iterator,
						null,
						scanCallback));
	}

	private List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final int limit ) {

		final List<ByteArrayId> result = new ArrayList<ByteArrayId>();
		try {
			if (options.isUseAltIndex() && operations.tableExists(tableName)) {
				for (final ByteArrayId dataId : dataIds) {
					final Scan scanner = new Scan();
					scanner.setStartRow(dataId.getBytes());
					scanner.setStopRow(dataId.getBytes());
					scanner.addFamily(adapterId.getBytes());

					final ResultScanner results = operations.getScannedResults(
							scanner,
							tableName);
					final Iterator<Result> iterator = results.iterator();
					int i = 0;
					while (iterator.hasNext() && (i < limit)) {
						result.add(new ByteArrayId(
								CellUtil.cloneQualifier(iterator.next().listCells().get(
										0))));
						i++;
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}

		return result;
	}

	public void store(
			final PrimaryIndex index ) {
		if (options.isPersistIndex() && !indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (options.isPersistAdapter() && !adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
	}

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
				final List<ByteArrayId> adapterIdsToQuery = new ArrayList<ByteArrayId>();

				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {
					if (sanitizedQuery instanceof RowIdQuery) {
						final HBaseRowIdsQuery<Object> q = new HBaseRowIdsQuery<Object>(
								adapter,
								indexAdapterPair.getLeft(),
								((RowIdQuery) sanitizedQuery).getRowIds(),
								(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
								filter,
								sanitizedQueryOptions.getAuthorizations());

						results.add(q.query(
								operations,
								tempAdapterStore,
								// TODO support subsampling
								// sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
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
						// TODO: support this
						// final PrefixIdQuery prefixIdQuery = (PrefixIdQuery)
						// sanitizedQuery;
						// final HBaseRowPrefixQuery<Object> prefixQuery = new
						// HBaseRowPrefixQuery<Object>(
						// indexAdapterPair.getLeft(),
						// prefixIdQuery.getRowPrefix(),
						// (ScanCallback<Object>)
						// sanitizedQueryOptions.getScanCallback(),
						// sanitizedQueryOptions.getLimit(),
						// sanitizedQueryOptions.getAuthorizations());
						// results.add(prefixQuery.query(
						// operations,
						// sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
						// tempAdapterStore));
						continue;
					}
					adapterIdsToQuery.add(adapter.getAdapterId());
				}

				// supports querying multiple adapters in a single index
				// in one query instance (one scanner) for efficiency
				if (adapterIdsToQuery.size() > 0) {
					HBaseConstraintsQuery hbaseQuery;
					hbaseQuery = new HBaseConstraintsQuery(
							adapterIdsToQuery,
							indexAdapterPair.getLeft(),
							sanitizedQuery,
							filter,
							sanitizedQueryOptions.getScanCallback(),
							queryOptions.getAggregation(),
							// TODO support field subsetting
							// queryOptions.getFieldIds(),
							sanitizedQueryOptions.getAuthorizations());

					results.add(hbaseQuery.query(
							operations,
							tempAdapterStore,
							// TODO support subsampling
							// sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
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

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			try {
				// TODO These interfaces should all provide remove and removeAll
				// capabilities instead of having to clear the
				// AbstractPersistence's cache manually
				((AbstractHBasePersistence) indexStore).clearCache();
				((AbstractHBasePersistence) adapterStore).clearCache();
				((AbstractHBasePersistence) statisticsStore).clearCache();
				// secondaryIndexDataStore.removeAll();
				((AbstractHBasePersistence) indexMappingStore).clearCache();

				operations.deleteAll();
				return true;
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to delete all tables",
						e);
				return false;
			}
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
				final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
				final String altIdxTableName = tableName + HBaseUtils.ALT_INDEX_TABLE;

				final boolean useAltIndex = options.isUseAltIndex() && operations.tableExists(altIdxTableName);

				final HBaseWriter idxDeleter = operations.createWriter(
						tableName,
						"",
						false);
				final HBaseWriter altIdxDelete = useAltIndex ? operations.createWriter(
						altIdxTableName,
						"",
						false) : null;

				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

					final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore,
							queriedAdapters.add(adapter.getAdapterId()));

					callbackCache.setPersistStats(options.isPersistDataStatistics());
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
							catch (final IOException e) {
								LOGGER.error(
										"Failed deletion",
										e);
								aOk.set(false);
							}

						}
					};

					CloseableIterator<?> dataIt = null;
					if (query instanceof RowIdQuery) {
						final HBaseRowIdsQuery<Object> q = new HBaseRowIdsQuery<Object>(
								adapter,
								index,
								((RowIdQuery) query).getRowIds(),
								callback,
								null,
								queryOptions.getAuthorizations());

						dataIt = q.query(
								operations,
								adapterStore,
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
						// TODO support this
						// dataIt = new HBaseRowPrefixQuery<Object>(
						// index,
						// ((PrefixIdQuery) query).getRowPrefix(),
						// callback,
						// null,
						// queryOptions.getAuthorizations()).query(
						// operations,
						// null,
						// adapterStore);

					}
					else {
						dataIt = new HBaseConstraintsQuery(
								Collections.singletonList(adapter.getAdapterId()),
								index,
								query,
								null,
								callback,
								null,
								// TODO support field subsetting
								// queryOptions.getFieldIds(),
								queryOptions.getAuthorizations()).query(
								operations,
								adapterStore,
								null);
					}

					if (dataIt != null) {
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
	}

	private <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final String... additionalAuthorizations )
			throws IOException {
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + HBaseUtils.ALT_INDEX_TABLE;
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
		deleteAll(
				altIdxTableName,
				adapterId,
				additionalAuthorizations);
	}

	private boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		HBaseWriter deleter = null;
		try {
			deleter = operations.createWriter(
					tableName,
					columnFamily);
			final Scan scanner = new Scan();
			try (ResultScanner results = operations.getScannedResults(
					scanner,
					tableName)) {
				for (final Result r : results) {
					final Delete delete = new Delete(
							r.getRow());
					delete.addFamily(StringUtils.stringToBinary(columnFamily));

					deleter.delete(delete);
				}
			}
			return true;
		}
		catch (final IOException e) {
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
			final HBaseWriter deleter,
			final List<ByteArrayId> ids )
			throws IOException {
		final List<Delete> deletes = new ArrayList<Delete>();
		for (final ByteArrayId id : ids) {
			deletes.add(new Delete(
					id.getBytes()));
		}
		deleter.delete(deletes);
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
		return HBaseMRUtils.getSplits(
				operations,
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
		return new GeoWaveHBaseRecordReader(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore,
				operations);
	}

	private class AltIndexCallback<T> implements
			IngestCallback<T>,
			Closeable,
			Flushable
	{

		private final WritableDataAdapter<T> adapter;
		private HBaseWriter altIdxWriter;
		private final String altIdxTableName;

		public AltIndexCallback(
				final String indexName,
				final WritableDataAdapter<T> adapter,
				final HBaseOptions hbaseOptions )
				throws IOException {
			this.adapter = adapter;
			altIdxTableName = indexName + HBaseUtils.ALT_INDEX_TABLE;
			if (operations.tableExists(indexName)) {
				if (!operations.tableExists(altIdxTableName)) {
					throw new TableNotFoundException(
							altIdxTableName);
				}
			}
			else {
				// index table does not exist yet
				if (operations.tableExists(altIdxTableName)) {
					operations.deleteTable(altIdxTableName);
					LOGGER.warn("Deleting current alternate index table [" + altIdxTableName
							+ "] as main table does not yet exist.");
				}
			}

			altIdxWriter = operations.createWriter(
					altIdxTableName,
					adapter.getAdapterId().getString(),
					hbaseOptions.isCreateTable());
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
			HBaseUtils.writeAltIndex(
					adapter,
					entryInfo,
					entry,
					altIdxWriter);

		}

		@Override
		public void flush() {
			// HBase writer does not require/support flush
		}

	}

}
