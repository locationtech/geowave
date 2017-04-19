package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.base.Deleter;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.DecodePackage;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveAccumuloRecordReader;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowPrefixQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.SingleEntryFilterIterator;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
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
public class AccumuloDataStore extends
		BaseDataStore implements
		MapReduceDataStore
{
	public final static String TYPE = "accumulo";

	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStore.class);

	private final AccumuloOperations accumuloOperations;
	private final AccumuloOptions accumuloOptions;

	private final AccumuloSplitsProvider splitsProvider = new AccumuloSplitsProvider();

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
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
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
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				accumuloOperations,
				accumuloOptions);

		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		secondaryIndexDataStore.setDataStore(this);
	}

	@Override
	protected IndexWriter createIndexWriter(
			final DataAdapter adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable ) {
		return new AccumuloIndexWriter(
				this,
				adapter,
				index,
				accumuloOperations,
				accumuloOptions,
				callback,
				closable);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {

		final String indexName = index.getId().getString();

		try {
			if (adapter instanceof RowMergingDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID).add(
						adapter.getAdapterId(),
						indexName)) {
					AccumuloUtils.attachRowMergingIterators(
							((RowMergingDataAdapter<?, ?>) adapter),
							accumuloOperations,
							accumuloOptions,
							index.getIndexStrategy().getNaturalSplits(),
							indexName);
				}
			}

			final byte[] adapterId = adapter.getAdapterId().getBytes();
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

	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		try {
			callbacks.add(new AltIndexCallback<T>(
					indexName,
					(WritableDataAdapter<T>) adapter,
					primaryIndexId));

		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to create table table for alt index to  [" + indexName + "]",
					e);
		}
	}

	private class AltIndexCallback<T> implements
			IngestCallback<T>
	{
		private final ByteArrayId EMPTY_VISIBILITY = new ByteArrayId(
				new byte[0]);
		private final ByteArrayId EMPTY_FIELD_ID = new ByteArrayId(
				new byte[0]);
		private final WritableDataAdapter<T> adapter;
		private final String altIdxTableName;
		private final ByteArrayId primaryIndexId;
		private final ByteArrayId altIndexId;

		public AltIndexCallback(
				final String indexName,
				final WritableDataAdapter<T> adapter,
				final ByteArrayId primaryIndexId )
				throws TableNotFoundException {
			this.adapter = adapter;
			altIdxTableName = indexName + ALT_INDEX_TABLE;
			altIndexId = new ByteArrayId(
					altIdxTableName);
			this.primaryIndexId = primaryIndexId;
			try {
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
			}
			catch (final IOException e) {
				LOGGER.error("Exception checking for index " + indexName + ": " + e);
			}
		}

		@Override
		public void entryIngested(
				final DataStoreEntryInfo entryInfo,
				final T entry ) {
			for (final ByteArrayId primaryIndexRowId : entryInfo.getRowIds()) {
				final ByteArrayId dataId = adapter.getDataId(entry);
				if ((dataId != null) && (dataId.getBytes() != null) && (dataId.getBytes().length > 0)) {
					secondaryIndexDataStore.storeJoinEntry(
							altIndexId,
							dataId,
							adapter.getAdapterId(),
							EMPTY_FIELD_ID,
							primaryIndexId,
							primaryIndexRowId,
							EMPTY_VISIBILITY);
				}
			}
		}
	}

	@Override
	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final AccumuloConstraintsQuery accumuloQuery = new AccumuloConstraintsQuery(
				this,
				adapterIdsToQuery,
				index,
				sanitizedQuery,
				filter,
				sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getAggregation(),
				sanitizedQueryOptions.getFieldIdsAdapterPair(),
				IndexMetaDataSet.getIndexMetadata(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DuplicateEntryCount.getDuplicateCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return accumuloQuery.query(
				accumuloOperations,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	@Override
	protected CloseableIterator<Object> queryRowPrefix(
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> adapterIdsToQuery ) {
		final AccumuloRowPrefixQuery<Object> prefixQuery = new AccumuloRowPrefixQuery<Object>(
				this,
				index,
				rowPrefix,
				(ScanCallback<Object, Object>) sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getLimit(),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());
		return prefixQuery.query(
				accumuloOperations,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				tempAdapterStore);
	}

	@Override
	protected CloseableIterator<Object> queryRowIds(
			final DataAdapter<Object> adapter,
			final PrimaryIndex index,
			final List<ByteArrayId> rowIds,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final AccumuloRowIdsQuery<Object> q = new AccumuloRowIdsQuery<Object>(
				this,
				adapter,
				index,
				rowIds,
				(ScanCallback<Object, Object>) sanitizedQueryOptions.getScanCallback(),
				filter,
				sanitizedQueryOptions.getAuthorizations());

		return q.query(
				accumuloOperations,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "i is part of loop condition")
	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object, Object> scanCallback,
			final DedupeFilter dedupeFilter,
			final String... authorizations ) {
		try {
			final ScannerBase scanner = accumuloOperations.createScanner(
					index.getId().getString(),
					authorizations);
			final DifferingFieldVisibilityEntryCount visibilityCount = DifferingFieldVisibilityEntryCount
					.getVisibilityCounts(
							index,
							Collections.singletonList(adapter.getAdapterId()),
							statisticsStore,
							authorizations);
			scanner.fetchColumnFamily(new Text(
					adapter.getAdapterId().getBytes()));
			if (visibilityCount.isAnyEntryDifferingFieldVisiblity()) {
				final IteratorSetting rowIteratorSettings = new IteratorSetting(
						SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
						SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
						WholeRowIterator.class);
				scanner.addScanIterator(rowIteratorSettings);

			}
			final IteratorSetting filterIteratorSettings = new IteratorSetting(
					SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_PRIORITY,
					SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_NAME,
					SingleEntryFilterIterator.class);

			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.ADAPTER_ID,
					ByteArrayUtils.byteArrayToString(adapter.getAdapterId().getBytes()));

			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.WHOLE_ROW_ENCODED_KEY,
					Boolean.toString(visibilityCount.isAnyEntryDifferingFieldVisiblity()));
			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.DATA_IDS,
					SingleEntryFilterIterator.encodeIDs(dataIds));
			scanner.addScanIterator(filterIteratorSettings);

			return new CloseableIteratorWrapper<Object>(
					new ScannerClosableWrapper(
							scanner),
					new AccumuloEntryIteratorWrapper(
							visibilityCount.isAnyEntryDifferingFieldVisiblity(),
							this,
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

	@Override
	protected List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {

		final List<ByteArrayId> result = new ArrayList<ByteArrayId>();
		try {
			if (accumuloOptions.isUseAltIndex() && accumuloOperations.tableExists(tableName)) {
				ScannerBase scanner = null;
				for (final ByteArrayId dataId : dataIds) {
					try {
						scanner = accumuloOperations.createScanner(
								tableName,
								authorizations);

						((Scanner) scanner).setRange(Range.exact(new Text(
								dataId.getBytes())));

						scanner.fetchColumnFamily(new Text(
								adapterId.getBytes()));

						final Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
						while (iterator.hasNext()) {
							final byte[] cq = iterator.next().getKey().getColumnQualifierData().getBackingArray();
							result.add(SecondaryIndexUtils.getPrimaryRowId(cq));
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
		}
		catch (final IOException e) {
			LOGGER.error("Exception checking for table " + tableName + ": " + e);
		}

		return result;
	}

	@Override
	protected boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		return accumuloOperations.deleteAll(
				tableName,
				columnFamily,
				additionalAuthorizations);
	}

	private static class ClosableBatchDeleter implements
			Deleter<Object>
	{
		private final BatchDeleter deleter;
		private final boolean isAltIndex;

		public ClosableBatchDeleter(
				final BatchDeleter deleter,
				final boolean isAltIndex ) {
			this.deleter = deleter;
			this.isAltIndex = isAltIndex;
		}

		@Override
		public void close() {
			deleter.close();
		}

		public BatchDeleter getDeleter() {
			return deleter;
		}

		@Override
		public void delete(
				final DataStoreEntryInfo entry,
				final Object nativeDataStoreEntry,
				final DataAdapter<?> adapter ) {
			final List<Range> rowRanges = new ArrayList<Range>();
			if (isAltIndex) {
				rowRanges.add(Range.exact(new Text(
						entry.getDataId())));
			}
			else {
				final List<ByteArrayId> rowIds = entry.getRowIds();
				for (final ByteArrayId id : rowIds) {
					rowRanges.add(Range.exact(new Text(
							id.getBytes())));
				}
			}
			final BatchDeleter batchDeleter = getDeleter();
			batchDeleter.setRanges(rowRanges);
			try {
				batchDeleter.delete();
			}
			catch (MutationsRejectedException | TableNotFoundException e) {
				LOGGER.warn(
						"Unable to delete row: " + entry.toString(),
						e);
			}
		}
	}

	@Override
	protected Deleter createIndexDeleter(
			final String indexTableName,
			final boolean isAltIndex,
			final String... authorizations )
			throws Exception {
		return new ClosableBatchDeleter(
				accumuloOperations.createBatchDeleter(
						indexTableName,
						authorizations),
				isAltIndex);
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
		return splitsProvider.getSplits(
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
				this,
				accumuloOperations);
	}

	@Override
	protected Iterable<GeoWaveRow> getRowsFromIngest(
			byte[] adapterId,
			DataStoreEntryInfo ingestInfo,
			List<FieldInfo<?>> fieldInfoList,
			boolean unused ) {
		ArrayList<GeoWaveRow> rows = new ArrayList<>();

		for (ByteArrayId rowId : ingestInfo.getRowIds()) {
			AccumuloRow accumuloRow = new AccumuloRow(
					rowId.getBytes(),
					fieldInfoList);
			rows.add(accumuloRow);
		}

		return rows;
	}

	@Override
	protected void verifyVisibility(
			VisibilityWriter customFieldVisibilityWriter,
			DataStoreEntryInfo ingestInfo ) {
		try {
			if (customFieldVisibilityWriter != DataStoreUtils.UNCONSTRAINED_VISIBILITY) {
				for (final FieldInfo field : ingestInfo.getFieldInfo()) {
					if ((field.getVisibility() != null) && (field.getVisibility().length > 0)) {
						accumuloOperations.insureAuthorization(
								null,
								StringUtils.stringFromBinary(field.getVisibility()));

					}
				}
			}

		}
		catch (AccumuloException e1) {
			LOGGER.error(e1);
		}
		catch (AccumuloSecurityException e2) {
			LOGGER.error(e2);
		}
	}

	@Override
	public void write(
			Writer writer,
			Iterable<GeoWaveRow> rows,
			final String unused ) {
		final List<Mutation> mutations = new ArrayList<Mutation>();

		for (GeoWaveRow geoWaveRow : rows) {
			AccumuloRow accumuloRow = (AccumuloRow) geoWaveRow;

			byte[] rowId = accumuloRow.getRowId();
			byte[] adapterId = accumuloRow.getAdapterId();

			final Mutation mutation = new Mutation(
					new Text(
							rowId));
			for (final FieldInfo<?> fieldInfo : accumuloRow.getFieldInfoList()) {
				if ((fieldInfo.getVisibility() != null) && (fieldInfo.getVisibility().length > 0)) {
					mutation.put(
							new Text(
									adapterId),
							new Text(
									fieldInfo.getDataValue().getId().getBytes()),
							new ColumnVisibility(
									fieldInfo.getVisibility()),
							new Value(
									fieldInfo.getWrittenValue()));
				}
				else {
					mutation.put(
							new Text(
									adapterId),
							new Text(
									fieldInfo.getDataValue().getId().getBytes()),
							new Value(
									fieldInfo.getWrittenValue()));
				}
			}

			mutations.add(mutation);
		}

		writer.write(mutations);
	}

	@Override
	public Object decodeRow(
			Object rowInput,
			boolean wholeRowEncoding,
			QueryFilter clientFilter,
			DataAdapter dataAdapter,
			final AdapterStore adapterStore,
			PrimaryIndex index,
			ScanCallback scanCallback,
			byte[] fieldSubsetBitmask,
			boolean decodeRow ) {
		if ((dataAdapter == null) && (adapterStore == null)) {
			LOGGER.error("Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}

		if (!(rowInput instanceof KeyValue)) {
			return null;
		}

		Key key = ((KeyValue) rowInput).getKey();
		Value value = ((KeyValue) rowInput).getValue();

		Map<Key, Value> rowMapping;
		if (wholeRowEncoding) {
			try {
				rowMapping = WholeRowIterator.decodeRow(
						key,
						value);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Could not decode row from iterator. Ensure whole row iterators are being used.",
						e);
				return null;
			}
		}
		else {
			rowMapping = new HashMap<Key, Value>();
			rowMapping.put(
					key,
					value);
		}

		DecodePackage decodePackage = new DecodePackage(
				index,
				decodeRow);

		ByteArrayId adapterId = null;
		if (dataAdapter == null) {
			Entry<Key, Value> firstEntry = (Entry<Key, Value>) rowMapping.entrySet().toArray()[0];
			adapterId = new ByteArrayId(
					firstEntry.getKey().getColumnFamilyData().getBackingArray());
		}

		if (!decodePackage.setOrRetrieveAdapter(
				dataAdapter,
				adapterId,
				adapterStore)) {
			LOGGER.error("Could not retrieve adapter from adapter store.");
			return null;
		}

		for (final Entry<Key, Value> entry : rowMapping.entrySet()) {
			if (!decodePackage.isAdapterVerified()) {
				adapterId = new ByteArrayId(
						entry.getKey().getColumnFamilyData().getBackingArray());

				if (!decodePackage.verifyAdapter(adapterId)) {
					LOGGER.error("Adapter verify failed: adapter does not match data.");
					return null;
				}
			}

			readFieldInfo(
					decodePackage,
					entry.getKey().getColumnQualifierData().getBackingArray(),
					entry.getKey().getColumnVisibilityData().getBackingArray(),
					entry.getValue().get());
		}

		AccumuloRow tempRow = new AccumuloRow(
				key.getRow().copyBytes(),
				decodePackage.getFieldInfo());

		return getDecodedRow(
				tempRow,
				decodePackage,
				clientFilter,
				scanCallback);
	}
}
