/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseRecordReader;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.HBaseSplitsProvider;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseAdapterStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseDataStatisticsStore;
import mil.nga.giat.geowave.datastore.hbase.metadata.HBaseIndexStore;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseConstraintsDelete;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseConstraintsQuery;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseRowIdsQuery;
import mil.nga.giat.geowave.datastore.hbase.query.HBaseRowPrefixQuery;
import mil.nga.giat.geowave.datastore.hbase.query.SingleEntryFilter;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils.MultiScannerClosableWrapper;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

public class HBaseDataStore extends
		BaseDataStore implements
		MapReduceDataStore
{
	public final static String TYPE = "hbase";

	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseDataStore.class);

	private final BasicHBaseOperations operations;
	private final HBaseOptions options;

	private final HBaseSplitsProvider splitsProvider = new HBaseSplitsProvider();

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
				new HBaseSecondaryIndexDataStore(
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
						operations),
				operations,
				options);
	}

	public HBaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final HBaseSecondaryIndexDataStore secondaryIndexDataStore,
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
			final HBaseSecondaryIndexDataStore secondaryIndexDataStore,
			final BasicHBaseOperations operations,
			final HBaseOptions options ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options);

		this.operations = operations;
		this.options = options;
		secondaryIndexDataStore.setDataStore(this);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {}

	@Override
	protected IndexWriter createIndexWriter(
			final DataAdapter adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable ) {
		return new HBaseIndexWriter(
				adapter,
				index,
				operations,
				options,
				callback,
				closable);
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
					options));

		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to create table table for alt index to  [" + indexName + "]",
					e);
		}
	}

	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object> scanCallback,
			final DedupeFilter dedupeFilter,
			final String[] authorizations,
			boolean delete ) {

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());

		final List<Iterator<Result>> resultList = new ArrayList<Iterator<Result>>();
		final List<ResultScanner> resultScanners = new ArrayList<ResultScanner>();
		Iterator<Result> iterator = null;

		try {
			final Scan scanner = new Scan();
			scanner.setMaxVersions(1);

			scanner.addFamily(adapter.getAdapterId().getBytes());

			if (options.isEnableCustomFilters()) {
				final FilterList filterList = new FilterList();

				for (final ByteArrayId dataId : dataIds) {
					filterList.addFilter(new SingleEntryFilter(
							dataId.getBytes(),
							adapter.getAdapterId().getBytes()));
				}

				if (!filterList.getFilters().isEmpty()) {
					scanner.setFilter(filterList);
				}
			}

			final ResultScanner results = operations.getScannedResults(
					scanner,
					tableName,
					authorizations);

			Iterator<Result> resultIt;

			if (!options.isEnableCustomFilters()) {
				ArrayList<Result> filteredResults = new ArrayList<Result>();

				for (Result result = results.next(); result != null; result = results.next()) {
					byte[] rowId = result.getRow();

					if (rowHasData(
							rowId,
							dataIds)) {
						filteredResults.add(result);
					}
				}

				resultIt = filteredResults.iterator();
			}
			else {
				resultIt = results.iterator();
			}

			resultScanners.add(results);

			if (resultIt.hasNext()) {
				resultList.add(resultIt);
			}

			iterator = Iterators.concat(resultList.iterator());
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to query table '" + tableName + "'.  Table does not exist.",
					e);
		}

		return new CloseableIteratorWrapper<Object>(
				new MultiScannerClosableWrapper(
						resultScanners),
				new HBaseEntryIteratorWrapper(
						tempAdapterStore,
						index,
						iterator,
						dedupeFilter,
						scanCallback,
						null,
						null,
						true,
						false));
	}

	protected boolean rowHasData(
			final byte[] rowId,
			final List<ByteArrayId> dataIds )
			throws IOException {

		final byte[] metadata = Arrays.copyOfRange(
				rowId,
				rowId.length - 12,
				rowId.length);

		final ByteBuffer metadataBuf = ByteBuffer.wrap(metadata);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				rowId,
				0,
				rowId.length - 12);
		final byte[] indexId = new byte[rowId.length - 12 - adapterIdLength - dataIdLength];
		final byte[] rawAdapterId = new byte[adapterIdLength];
		final byte[] rawDataId = new byte[dataIdLength];
		buf.get(indexId);
		buf.get(rawAdapterId);
		buf.get(rawDataId);

		for (ByteArrayId dataId : dataIds) {
			if (Arrays.equals(
					rawDataId,
					dataId.getBytes())) {
				return true;
			}
		}

		return false;
	}

	@Override
	protected List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {

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
							tableName,
							authorizations);
					final Iterator<Result> iterator = results.iterator();
					while (iterator.hasNext()) {
						result.add(new ByteArrayId(
								CellUtil.cloneQualifier(iterator.next().listCells().get(
										0))));
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

	@Override
	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore,
			boolean delete ) {

		final HBaseConstraintsQuery hbaseQuery;

		if (!delete) {
			hbaseQuery = new HBaseConstraintsQuery(
					adapterIdsToQuery,
					index,
					sanitizedQuery,
					filter,
					sanitizedQueryOptions.getScanCallback(),
					sanitizedQueryOptions.getAggregation(),
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
					sanitizedQueryOptions.getFieldIdsAdapterPair(),
					sanitizedQueryOptions.getAuthorizations());

		}
		else {
			hbaseQuery = new HBaseConstraintsDelete(
					adapterIdsToQuery,
					index,
					sanitizedQuery,
					filter,
					sanitizedQueryOptions.getScanCallback(),
					sanitizedQueryOptions.getAggregation(),
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
					sanitizedQueryOptions.getFieldIdsAdapterPair(),
					sanitizedQueryOptions.getAuthorizations());
		}

		hbaseQuery.setOptions(options);

		return hbaseQuery.query(
				operations,
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
		final HBaseRowPrefixQuery<Object> prefixQuery = new HBaseRowPrefixQuery<Object>(
				index,
				rowPrefix,
				(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getAuthorizations());

		prefixQuery.setOptions(options);

		return prefixQuery.query(
				operations,
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
		final HBaseRowIdsQuery<Object> q = new HBaseRowIdsQuery<Object>(
				adapter,
				index,
				rowIds,
				(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
				filter,
				sanitizedQueryOptions.getAuthorizations());

		q.setOptions(options);

		return q.query(
				operations,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				-1);
	}

	@Override
	protected void addToBatch(
			final Closeable idxDeleter,
			final List<ByteArrayId> rowIds )
			throws Exception {
		final List<Delete> deletes = new ArrayList<Delete>();
		for (final ByteArrayId id : rowIds) {
			deletes.add(new Delete(
					id.getBytes()));
		}
		if (idxDeleter instanceof HBaseWriter) {
			((HBaseWriter) idxDeleter).delete(deletes);
		}
	}

	@Override
	protected Closeable createIndexDeleter(
			final String indexTableName,
			final String[] authorizations )
			throws Exception {
		return operations.createWriter(
				indexTableName,
				new String[] {},
				false);
	}

	@Override
	protected boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		HBaseWriter deleter = null;
		try {
			deleter = operations.createWriter(
					tableName,
					new String[] {},
					false);
			final Scan scanner = new Scan();
			try (ResultScanner results = operations.getScannedResults(
					scanner,
					tableName,
					additionalAuthorizations)) {
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
			altIdxTableName = indexName + ALT_INDEX_TABLE;
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
					new String[] {
						adapter.getAdapterId().getString()
					},
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
