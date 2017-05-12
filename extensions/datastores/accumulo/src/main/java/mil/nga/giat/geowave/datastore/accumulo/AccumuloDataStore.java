package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
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
import mil.nga.giat.geowave.core.store.base.CastIterator;
import mil.nga.giat.geowave.core.store.base.DataStoreCallbackManager;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.core.store.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveAccumuloRecordReader;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsDelete;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowIdsDelete;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowPrefixDelete;
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

	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStore.class);

	private final AccumuloOperations accumuloOperations;
	private final AccumuloOptions accumuloOptions;

	private final AccumuloSplitsProvider splitsProvider = new AccumuloSplitsProvider();

	private static class DupTracker
	{
		HashMap<ByteArrayId, ByteArrayId> idMap;
		HashMap<ByteArrayId, Integer> dupCountMap;

		public DupTracker() {
			idMap = new HashMap<>();
			dupCountMap = new HashMap<>();
		}
	}

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
			final AdapterStore tempAdapterStore,
			boolean delete ) {
		final AccumuloConstraintsQuery accumuloQuery;
		if (delete) {
			accumuloQuery = new AccumuloConstraintsDelete(
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
		}
		else {
			accumuloQuery = new AccumuloConstraintsQuery(
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
		}
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
			final List<ByteArrayId> adapterIdsToQuery,
			boolean delete ) {
		final AccumuloRowPrefixQuery<Object> prefixQuery;
		if (delete) {
			prefixQuery = new AccumuloRowPrefixDelete<Object>(
					index,
					rowPrefix,
					(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
					sanitizedQueryOptions.getLimit(),
					DifferingFieldVisibilityEntryCount.getVisibilityCounts(
							index,
							adapterIdsToQuery,
							statisticsStore,
							sanitizedQueryOptions.getAuthorizations()),
					sanitizedQueryOptions.getAuthorizations());
		}
		else {
			prefixQuery = new AccumuloRowPrefixQuery<Object>(
					index,
					rowPrefix,
					(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
					sanitizedQueryOptions.getLimit(),
					DifferingFieldVisibilityEntryCount.getVisibilityCounts(
							index,
							adapterIdsToQuery,
							statisticsStore,
							sanitizedQueryOptions.getAuthorizations()),
					sanitizedQueryOptions.getAuthorizations());
		}
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
			final AdapterStore tempAdapterStore,
			boolean delete ) {
		final AccumuloRowIdsQuery<Object> q;
		if (delete) {
			q = new AccumuloRowIdsDelete<Object>(
					adapter,
					index,
					rowIds,
					(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
					filter,
					sanitizedQueryOptions.getAuthorizations());
		}
		else {
			q = new AccumuloRowIdsQuery<Object>(
					adapter,
					index,
					rowIds,
					(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
					filter,
					sanitizedQueryOptions.getAuthorizations());
		}
		return q.query(
				accumuloOperations,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	protected static byte[] getRowIdBytes(
			final GeowaveRowId rowElements ) {
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

	protected static GeowaveRowId getRowIdObject(
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
		return new GeowaveRowId(
				indexId,
				dataId,
				adapterId,
				numberOfDuplicates);
	}

	@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "i is part of loop condition")
	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object> scanCallback,
			final DedupeFilter dedupeFilter,
			final String[] authorizations,
			boolean delete ) {

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

	private static class ClosableBatchDeleter implements
			Closeable
	{
		private final BatchDeleter deleter;

		public ClosableBatchDeleter(
				final BatchDeleter deleter ) {
			this.deleter = deleter;
		}

		@Override
		public void close() {
			deleter.close();
		}

		public BatchDeleter getDeleter() {
			return deleter;
		}
	}

	@Override
	protected Closeable createIndexDeleter(
			final String indexTableName,
			final String[] authorizations )
			throws Exception {
		return new ClosableBatchDeleter(
				accumuloOperations.createBatchDeleter(
						indexTableName,
						authorizations));
	}

	@Override
	protected void addToBatch(
			final Closeable deleter,
			final List<ByteArrayId> ids )
			throws Exception {
		final List<Range> rowRanges = new ArrayList<Range>();
		for (final ByteArrayId id : ids) {
			rowRanges.add(Range.exact(new Text(
					id.getBytes())));
		}
		if (deleter instanceof ClosableBatchDeleter) {
			final BatchDeleter batchDeleter = ((ClosableBatchDeleter) deleter).getDeleter();
			batchDeleter.setRanges(rowRanges);
			batchDeleter.delete();
		}
		else {
			LOGGER.error("Deleter incompatible with data store type");
		}
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
				accumuloOperations);
	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		// Normal mass wipeout
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			return deleteEverything();
		}

		// Delete by data ID works best the old way
		if (query instanceof DataIdQuery) {
			return super.delete(
					queryOptions,
					query);
		}

		// Clean up inputs
		final QueryOptions sanitizedQueryOptions = (queryOptions == null) ? new QueryOptions() : queryOptions;
		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		// Get the index-adapter pairs
		MemoryAdapterStore tempAdapterStore;
		List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> indexAdapterPairs;

		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));

			indexAdapterPairs = sanitizedQueryOptions.getAdaptersWithMinimalSetOfIndices(
					tempAdapterStore,
					indexMappingStore,
					indexStore);
		}
		catch (final IOException e1) {
			LOGGER.error(
					"Failed to resolve adapter or index for query",
					e1);

			return false;
		}

		// Setup the stats for update
		final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
				statisticsStore,
				secondaryIndexDataStore,
				true);

		callbackCache.setPersistStats(accumuloOptions.isPersistDataStatistics());

		final DupTracker dupTracker = new DupTracker();

		// Get BatchDeleters for the query
		CloseableIterator<Object> deleteIt = getBatchDeleters(
				callbackCache,
				tempAdapterStore,
				indexAdapterPairs,
				sanitizedQueryOptions,
				sanitizedQuery,
				dupTracker);

		// Iterate through deleters
		while (deleteIt.hasNext()) {
			deleteIt.next();
		}

		try {
			deleteIt.close();
			callbackCache.close();
		}
		catch (IOException e) {
			LOGGER.error(
					"Failed to close delete iterator",
					e);
		}

		// Have to delete dups by data ID if any
		if (!dupTracker.dupCountMap.isEmpty()) {
			LOGGER.warn("Need to delete duplicates by data ID");
			int dupDataCount = 0;
			int dupFailCount = 0;
			boolean deleteByIdSuccess = true;

			for (ByteArrayId dataId : dupTracker.dupCountMap.keySet()) {
				if (!super.delete(
						new QueryOptions(),
						new DataIdQuery(
								dupTracker.idMap.get(dataId),
								dataId))) {
					deleteByIdSuccess = false;
					dupFailCount++;
				}
				else {
					dupDataCount++;
				}
			}

			if (deleteByIdSuccess) {
				LOGGER.warn("Deleted " + dupDataCount + " duplicates by data ID");
			}
			else {
				LOGGER.warn("Failed to delete " + dupFailCount + " duplicates by data ID");
			}
		}

		boolean countAggregation = (sanitizedQuery instanceof DataIdQuery ? false : true);

		// Count after the delete. Should always be zero
		int undeleted = getCount(
				indexAdapterPairs,
				sanitizedQueryOptions,
				sanitizedQuery,
				countAggregation);

		// Expected outcome
		if (undeleted == 0) {
			return true;
		}

		LOGGER.warn("Accumulo bulk delete failed to remove " + undeleted + " rows");

		// Fallback: delete duplicates via callback using base delete method
		return super.delete(
				queryOptions,
				query);
	}

	protected int getCount(
			List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> indexAdapterPairs,
			final QueryOptions sanitizedQueryOptions,
			final Query sanitizedQuery,
			final boolean countAggregation ) {
		int count = 0;

		for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : indexAdapterPairs) {
			for (DataAdapter dataAdapter : indexAdapterPair.getRight()) {
				QueryOptions countOptions = new QueryOptions(
						sanitizedQueryOptions);
				if (countAggregation) {
					countOptions.setAggregation(
							new CountAggregation(),
							dataAdapter);
				}

				try (final CloseableIterator<Object> it = query(
						countOptions,
						sanitizedQuery)) {
					while (it.hasNext()) {
						if (countAggregation) {
							final CountResult result = ((CountResult) (it.next()));
							if (result != null) {
								count += result.getCount();
							}
						}
						else {
							it.next();
							count++;
						}
					}

					it.close();
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Error running count aggregation",
							e);
					return count;
				}
			}
		}

		return count;
	}

	protected <T> CloseableIterator<T> getBatchDeleters(
			final DataStoreCallbackManager callbackCache,
			final MemoryAdapterStore tempAdapterStore,
			final List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> indexAdapterPairs,
			final QueryOptions sanitizedQueryOptions,
			final Query sanitizedQuery,
			final DupTracker dupTracker ) {
		final boolean DELETE = true; // for readability
		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();

		for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : indexAdapterPairs) {
			final List<ByteArrayId> adapterIdsToQuery = new ArrayList<>();
			for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

				// Add scan callback for bookkeeping
				final ScanCallback<Object> callback = new ScanCallback<Object>() {
					@Override
					public void entryScanned(
							final DataStoreEntryInfo entryInfo,
							final Object entry ) {
						updateDupCounts(
								dupTracker,
								adapter.getAdapterId(),
								entryInfo);

						callbackCache.getDeleteCallback(
								(WritableDataAdapter<Object>) adapter,
								indexAdapterPair.getLeft()).entryDeleted(
								entryInfo,
								entry);
					}
				};

				sanitizedQueryOptions.setScanCallback(callback);

				if (sanitizedQuery instanceof RowIdQuery) {
					sanitizedQueryOptions.setLimit(-1);
					results.add(queryRowIds(
							adapter,
							indexAdapterPair.getLeft(),
							((RowIdQuery) sanitizedQuery).getRowIds(),
							null,
							sanitizedQueryOptions,
							tempAdapterStore,
							DELETE));
					continue;
				}
				else if (sanitizedQuery instanceof PrefixIdQuery) {
					final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
					results.add(queryRowPrefix(
							indexAdapterPair.getLeft(),
							prefixIdQuery.getRowPrefix(),
							sanitizedQueryOptions,
							tempAdapterStore,
							adapterIdsToQuery,
							DELETE));
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
						null,
						sanitizedQueryOptions,
						tempAdapterStore,
						DELETE));
			}
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

	protected void updateDupCounts(
			final DupTracker dupTracker,
			final ByteArrayId adapterId,
			DataStoreEntryInfo entryInfo ) {
		ByteArrayId dataId = new ByteArrayId(
				entryInfo.getDataId());

		for (ByteArrayId rowId : entryInfo.getRowIds()) {
			GeowaveRowId rowData = new GeowaveRowId(
					rowId.getBytes());
			int rowDups = rowData.getNumberOfDuplicates();

			if (rowDups > 0) {
				if (dupTracker.idMap.get(dataId) == null) {
					dupTracker.idMap.put(
							dataId,
							adapterId);
				}

				Integer mapDups = dupTracker.dupCountMap.get(dataId);

				if (mapDups == null) {
					dupTracker.dupCountMap.put(
							dataId,
							rowDups);
				}
				else if (mapDups == 1) {
					dupTracker.dupCountMap.remove(dataId);
				}
				else {
					dupTracker.dupCountMap.put(
							dataId,
							mapDups - 1);
				}
			}
		}

	}
}
