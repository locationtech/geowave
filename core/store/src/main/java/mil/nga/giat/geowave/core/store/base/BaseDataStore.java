package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallbackList;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.DecodePackage;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import mil.nga.giat.geowave.core.store.index.writer.IndexCompositeWriter;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public abstract class BaseDataStore implements
		DataStore
{
	private final static Logger LOGGER = Logger.getLogger(BaseDataStore.class);

	protected static final String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	protected final IndexStore indexStore;
	protected final AdapterStore adapterStore;
	protected final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
	protected final AdapterIndexMappingStore indexMappingStore;
	protected final DataStoreOperations baseOperations;
	protected final DataStoreOptions baseOptions;

	public BaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.indexMappingStore = indexMappingStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;

		baseOperations = operations;
		baseOptions = options;
	}

	public void store(
			final PrimaryIndex index ) {
		if (baseOptions.isPersistIndex() && !indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (baseOptions.isPersistAdapter() && !adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
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

			callbackManager.setPersistStats(baseOptions.isPersistDataStatistics());

			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();

			store(index);

			final String indexName = index.getId().getString();

			if (adapter instanceof WritableDataAdapter) {
				if (baseOptions.isUseAltIndex()) {
					addAltIndexCallback(
							callbacks,
							indexName,
							adapter,
							index.getId());
				}
			}
			callbacks.add(callbackManager.getIngestCallback(
					(WritableDataAdapter<T>) adapter,
					index));

			initOnIndexWriterCreate(
					adapter,
					index);

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<T>(
					callbacks);
			writers[i] = createIndexWriter(
					adapter,
					index,
					baseOperations,
					baseOptions,
					callbacksList,
					callbacksList);

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
						sanitizedQueryOptions.setLimit(-1);
						results.add(queryRowIds(
								adapter,
								indexAdapterPair.getLeft(),
								((RowIdQuery) sanitizedQuery).getRowIds(),
								filter,
								sanitizedQueryOptions,
								tempAdapterStore));
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
									(ScanCallback<Object, Object>) sanitizedQueryOptions.getScanCallback(),
									sanitizedQueryOptions.getAuthorizations(),
									sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension()));
						}
						continue;
					}
					else if (sanitizedQuery instanceof PrefixIdQuery) {
						final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
						results.add(queryRowPrefix(
								indexAdapterPair.getLeft(),
								prefixIdQuery.getRowPrefix(),
								sanitizedQueryOptions,
								tempAdapterStore,
								adapterIdsToQuery));
						continue;
					}
					adapterIdsToQuery.add(adapter.getAdapterId());
				}
				// supports querying multiple adapters in a single index
				// in one query instance (one scanner) for efficiency
				if (adapterIdsToQuery.size() > 0) {
					results.add(queryConstraints(
							adapterIdsToQuery,
							indexAdapterPair.getLeft(),
							sanitizedQuery,
							filter,
							sanitizedQueryOptions,
							tempAdapterStore));
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

	@SuppressWarnings("unchecked")
	protected CloseableIterator<Object> getEntries(
			final PrimaryIndex index,
			final List<ByteArrayId> dataIds,
			final DataAdapter<Object> adapter,
			final DedupeFilter dedupeFilter,
			final ScanCallback<Object, Object> callback,
			final String[] authorizations,
			final double[] maxResolutionSubsamplingPerDimension )
			throws IOException {
		final String altIdxTableName = index.getId().getString() + ALT_INDEX_TABLE;

		MemoryAdapterStore tempAdapterStore;

		tempAdapterStore = new MemoryAdapterStore(
				new DataAdapter[] {
					adapter
				});

		if (baseOptions.isUseAltIndex() && baseOperations.tableExists(altIdxTableName)) {
			final List<ByteArrayId> rowIds = getAltIndexRowIds(
					altIdxTableName,
					dataIds,
					adapter.getAdapterId());

			if (rowIds.size() > 0) {

				final QueryOptions options = new QueryOptions();
				options.setScanCallback(callback);
				options.setAuthorizations(authorizations);
				options.setMaxResolutionSubsamplingPerDimension(maxResolutionSubsamplingPerDimension);
				options.setLimit(-1);

				return queryRowIds(
						adapter,
						index,
						rowIds,
						dedupeFilter,
						options,
						tempAdapterStore);
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
					authorizations);
		}
		return new CloseableIterator.Empty();
	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			try {

				indexStore.removeAll();
				adapterStore.removeAll();
				statisticsStore.removeAll();
				secondaryIndexDataStore.removeAll();
				indexMappingStore.removeAll();

				baseOperations.deleteAll();
				return true;
			}
			catch (final Exception e) {
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
		Deleter idxDeleter = null, altIdxDeleter = null;
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
				final String altIdxTableName = indexTableName + ALT_INDEX_TABLE;

				idxDeleter = createIndexDeleter(
						indexTableName,
						false,
						queryOptions.getAuthorizations());

				altIdxDeleter = baseOptions.isUseAltIndex() && baseOperations.tableExists(altIdxTableName) ? createIndexDeleter(
						altIdxTableName,
						true,
						queryOptions.getAuthorizations())
						: null;

				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

					final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore,
							queriedAdapters.add(adapter.getAdapterId()));

					callbackCache.setPersistStats(baseOptions.isPersistDataStatistics());

					if (query instanceof EverythingQuery) {
						deleteEntries(
								adapter,
								index,
								queryOptions.getAuthorizations());
						continue;
					}
					final Deleter internalIdxDeleter = idxDeleter;
					final Deleter internalAltIdxDeleter = altIdxDeleter;
					final ScanCallback<Object, Object> callback = new ScanCallback<Object, Object>() {
						@Override
						public void entryScanned(
								final DataStoreEntryInfo entryInfo,
								final Object nativeDataStoreEntry,
								final Object entry ) {
							callbackCache.getDeleteCallback(
									(WritableDataAdapter<Object>) adapter,
									index).entryDeleted(
									entryInfo,
									entry);
							try {
								internalIdxDeleter.delete(
										entryInfo,
										nativeDataStoreEntry,
										adapter);
								if (internalAltIdxDeleter != null) {
									internalAltIdxDeleter.delete(
											entryInfo,
											nativeDataStoreEntry,
											adapter);
								}
							}
							catch (final Exception e) {
								LOGGER.error(
										"Failed deletion",
										e);
								aOk.set(false);
							}
						}
					};

					CloseableIterator<?> dataIt = null;
					queryOptions.setScanCallback(callback);
					final List<ByteArrayId> adapterIds = Collections.singletonList(adapter.getAdapterId());
					if (query instanceof RowIdQuery) {
						queryOptions.setLimit(-1);
						dataIt = queryRowIds(
								adapter,
								index,
								((RowIdQuery) query).getRowIds(),
								null,
								queryOptions,
								adapterStore);
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
								null);
					}
					else if (query instanceof PrefixIdQuery) {
						dataIt = queryRowPrefix(
								index,
								((PrefixIdQuery) query).getRowPrefix(),
								queryOptions,
								adapterStore,
								adapterIds);
					}
					else {
						dataIt = queryConstraints(
								adapterIds,
								index,
								query,
								null,
								queryOptions,
								adapterStore);
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
			}

			return aOk.get();
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed delete operation " + query.toString(),
					e);
			return false;
		}
		finally {
			try {
				if (idxDeleter != null) {
					idxDeleter.close();
				}
				if (altIdxDeleter != null) {
					altIdxDeleter.close();
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close deleter",
						e);
			}
		}

	}

	private <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final String... additionalAuthorizations )
			throws IOException {
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + ALT_INDEX_TABLE;
		final String adapterId = StringUtils.stringFromBinary(adapter.getAdapterId().getBytes());

		try (final CloseableIterator<DataStatistics<?>> it = statisticsStore.getDataStatistics(adapter.getAdapterId())) {

			while (it.hasNext()) {
				final DataStatistics<?> stats = it.next();
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
		if (baseOptions.isUseAltIndex() && baseOperations.tableExists(altIdxTableName)) {
			deleteAll(
					altIdxTableName,
					adapterId,
					additionalAuthorizations);
		}
	}

	protected abstract boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations );

	protected abstract Deleter createIndexDeleter(
			String indexTableName,
			boolean altIndex,
			String... authorizations )
			throws Exception;

	protected abstract List<ByteArrayId> getAltIndexRowIds(
			final String altIdxTableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations );

	protected abstract CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object, Object> callback,
			final DedupeFilter dedupeFilter,
			final String... authorizations );

	protected abstract CloseableIterator<Object> queryConstraints(
			List<ByteArrayId> adapterIdsToQuery,
			PrimaryIndex index,
			Query sanitizedQuery,
			DedupeFilter filter,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore );

	protected abstract CloseableIterator<Object> queryRowPrefix(
			PrimaryIndex index,
			ByteArrayId rowPrefix,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore,
			List<ByteArrayId> adapterIdsToQuery );

	protected abstract CloseableIterator<Object> queryRowIds(
			DataAdapter<Object> adapter,
			PrimaryIndex index,
			List<ByteArrayId> rowIds,
			DedupeFilter filter,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore );

	protected abstract <T> void addAltIndexCallback(
			List<IngestCallback<T>> callbacks,
			String indexName,
			DataAdapter<T> adapter,
			ByteArrayId primaryIndexId );

	protected abstract IndexWriter createIndexWriter(
			DataAdapter adapter,
			PrimaryIndex index,
			DataStoreOperations baseOperations,
			DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable );

	protected abstract void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index );

	/**
	 * General-purpose entry ingest
	 */
	public <T, R> DataStoreEntryInfo write(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index,
			final T entry,
			final Writer writer,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {

		final DataStoreEntryInfo ingestInfo = DataStoreUtils.getIngestInfo(
				writableAdapter,
				index,
				entry,
				customFieldVisibilityWriter);

		verifyVisibility(
				customFieldVisibilityWriter,
				ingestInfo);

		final Iterable<GeoWaveRow> rows = toGeoWaveRows(
				writableAdapter,
				index,
				ingestInfo);

		try {
			write(
					writer,
					rows,
					writableAdapter.getAdapterId().getString());
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Writing to table failed.",
					e);
		}

		return ingestInfo;
	}

	protected void verifyVisibility(
			VisibilityWriter customFieldVisibilityWriter,
			DataStoreEntryInfo ingestInfo ) {
		// TODO: Implement/override vis for all datastore types
	}

	/**
	 * DataStore-agnostic method to accept ingest info and return generic
	 * geowave rows
	 * 
	 * @param writableAdapter
	 * @param index
	 * @param ingestInfo
	 * 
	 * @return list of GeoWaveRow
	 */
	public <T> Iterable<GeoWaveRow> toGeoWaveRows(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index,
			final DataStoreEntryInfo ingestInfo ) {
		final List<FieldInfo<?>> fieldInfoList = DataStoreUtils.composeFlattenedFields(
				ingestInfo.getFieldInfo(),
				index.getIndexModel(),
				writableAdapter);

		final boolean ensureUniqueId = (writableAdapter instanceof RowMergingDataAdapter)
				&& (((RowMergingDataAdapter) writableAdapter).getTransform() != null);

		final Iterable<GeoWaveRow> geowaveRows = getRowsFromIngest(
				writableAdapter.getAdapterId().getBytes(),
				ingestInfo,
				fieldInfoList,
				ensureUniqueId);

		return geowaveRows;
	}

	/**
	 * DataStore-specific method to create its row impl on ingest Called from
	 * toGeoWaveRows method above.
	 * 
	 * @param adapterId
	 * @param ingestInfo
	 * @param fieldInfoList
	 * @param ensureUniqueId
	 * @return
	 */
	protected abstract Iterable<GeoWaveRow> getRowsFromIngest(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo,
			final List<FieldInfo<?>> fieldInfoList,
			final boolean ensureUniqueId );

	/**
	 * DataStore-specific method that accepts a writer and a list of rows,
	 * converts them to db mutations and passes them to the writer.
	 */
	public abstract void write(
			Writer writer,
			Iterable<GeoWaveRow> rows,
			final String columnFamily );

	/**
	 * Basic method that decodes a native row Currently overridden by Accumulo
	 * and HBase; Unification in progress
	 * 
	 * Override this method if you can't pass in a GeoWaveRow!
	 */
	public Object decodeRow(
			final Object inputRow,
			final boolean wholeRowEncoding,
			final QueryFilter clientFilter,
			final DataAdapter adapter,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final ScanCallback scanCallback,
			final byte[] fieldSubsetBitmask,
			final boolean decodeRow ) {
		// The base case for decoding requires the input row to be a GeoWaveRow
		if (!(inputRow instanceof GeoWaveRow)) {
			return null;
		}

		GeoWaveRow geowaveRow = (GeoWaveRow) inputRow;
		ByteArrayId adapterId = new ByteArrayId(
				geowaveRow.getAdapterId());

		DecodePackage decodePackage = new DecodePackage(
				index,
				true);

		if (!decodePackage.setOrRetrieveAdapter(
				adapter,
				adapterId,
				adapterStore)) {
			// Fail quietly? Some IT's hit this a lot
			// LOGGER.error("Could not decode row from iterator. Either adapter
			// or adapter store must be non-null.");
			return null;
		}

		byte[] byteValue = geowaveRow.getValue();
		byte[] fieldMask = geowaveRow.getFieldMask();

		if (fieldSubsetBitmask != null) {
			final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
					fieldMask,
					fieldSubsetBitmask);
			byteValue = BitmaskUtils.constructNewValue(
					byteValue,
					fieldMask,
					newBitmask);
			fieldMask = newBitmask;
		}

		readFieldInfo(
				decodePackage,
				fieldMask,
				DataStoreUtils.EMTPY_VISIBILITY, //
				byteValue);

		return getDecodedRow(
				geowaveRow,
				decodePackage,
				clientFilter,
				scanCallback);
	}

	/**
	 * Generic field reader - updates fieldInfoList from field input data
	 */
	protected void readFieldInfo(
			final DecodePackage decodePackage,
			final byte[] fieldMask,
			final byte[] commonVisiblity,
			final byte[] byteValue ) {
		final List<FlattenedFieldInfo> fieldInfos = DataStoreUtils.decomposeFlattenedFields(
				fieldMask,
				byteValue,
				commonVisiblity,
				-1).getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final ByteArrayId fieldId = decodePackage.getDataAdapter().getFieldIdForPosition(
					decodePackage.getIndex().getIndexModel(),
					fieldInfo.getFieldPosition());
			final FieldReader<? extends CommonIndexValue> indexFieldReader = decodePackage
					.getIndex()
					.getIndexModel()
					.getReader(
							fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(fieldInfo.getValue());
				indexValue.setVisibility(commonVisiblity);
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				decodePackage.getIndexData().addValue(
						val);
				decodePackage.getFieldInfo().add(
						DataStoreUtils.getFieldInfo(
								val,
								fieldInfo.getValue(),
								commonVisiblity));
			}
			else {
				final FieldReader<?> extFieldReader = decodePackage.getDataAdapter().getReader(
						fieldId);
				if (extFieldReader != null) {
					final Object value = extFieldReader.readField(fieldInfo.getValue());
					final PersistentValue<Object> val = new PersistentValue<Object>(
							fieldId,
							value);
					decodePackage.getExtendedData().addValue(
							val);
					decodePackage.getFieldInfo().add(
							DataStoreUtils.getFieldInfo(
									val,
									fieldInfo.getValue(),
									commonVisiblity));
				}
				else {
					LOGGER.error("field reader not found for data entry, the value may be ignored");
					decodePackage.getUnknownData().addValue(
							new PersistentValue<byte[]>(
									fieldId,
									fieldInfo.getValue()));
				}
			}
		}
	}

	/**
	 * build a persistence encoding object first, pass it through the client
	 * filters and if its accepted, use the data adapter to decode the
	 * persistence model into the native data type
	 */
	protected Object getDecodedRow(
			GeoWaveRow geowaveRow,
			DecodePackage decodePackage,
			QueryFilter clientFilter,
			ScanCallback scanCallback ) {
		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				decodePackage.getDataAdapter().getAdapterId(),
				new ByteArrayId(
						geowaveRow.getDataId()),
				new ByteArrayId(
						geowaveRow.getIndex()),
				geowaveRow.getNumberOfDuplicates(),
				decodePackage.getIndexData(),
				decodePackage.getUnknownData(),
				decodePackage.getExtendedData());

		if ((clientFilter == null) || clientFilter.accept(
				decodePackage.getIndex().getIndexModel(),
				encodedRow)) {
			if (!decodePackage.isDecodeRow()) {
				return encodedRow;
			}

			final Object decodedRow = decodePackage.getDataAdapter().decode(
					encodedRow,
					decodePackage.getIndex());

			if (scanCallback != null) {
				final DataStoreEntryInfo entryInfo = new DataStoreEntryInfo(
						geowaveRow.getDataId(),
						Arrays.asList(new ByteArrayId(
								geowaveRow.getIndex())),
						Arrays.asList(new ByteArrayId(
								geowaveRow.getRowId())),
						decodePackage.getFieldInfo());

				scanCallback.entryScanned(
						entryInfo,
						geowaveRow,
						decodedRow);
			}

			return decodedRow;
		}

		return null;
	}
}
