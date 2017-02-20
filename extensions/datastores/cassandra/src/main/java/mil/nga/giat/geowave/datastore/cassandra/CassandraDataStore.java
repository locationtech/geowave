package mil.nga.giat.geowave.datastore.cassandra;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.cassandra.index.secondary.CassandraSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraAdapterStore;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraDataStatisticsStore;
import mil.nga.giat.geowave.datastore.cassandra.metadata.CassandraIndexStore;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.query.CassandraConstraintsQuery;
import mil.nga.giat.geowave.datastore.cassandra.query.CassandraRowIdsQuery;
import mil.nga.giat.geowave.datastore.cassandra.query.CassandraRowPrefixQuery;

public class CassandraDataStore extends
		BaseDataStore
{
	private final static Logger LOGGER = Logger.getLogger(CassandraDataStore.class);
	public static final Integer PARTITIONS = 4;

	private final CassandraOperations operations;
	private static int counter = 0;

	public CassandraDataStore(
			final CassandraOperations operations ) {
		super(
				new CassandraIndexStore(
						operations),
				new CassandraAdapterStore(
						operations),
				new CassandraDataStatisticsStore(
						operations),
				new CassandraAdapterIndexMappingStore(
						operations),
				new CassandraSecondaryIndexDataStore(
						operations),
				operations,
				operations.getOptions());
		this.operations = operations;
	}

	@Override
	protected boolean deleteAll(
			final String tableName,
			final String adapterId,
			final String... additionalAuthorizations ) {
		return operations.deleteAll(
				tableName,
				new ByteArrayId(
						adapterId).getBytes(),
				additionalAuthorizations);
	}

	@Override
	protected Deleter createIndexDeleter(
			final String indexTableName,
			final boolean isAltIndex,
			final String... authorizations )
			throws Exception {
		return new CassandraRowDeleter(
				operations,
				indexTableName,
				authorizations);
	}

	@Override
	protected List<ByteArrayId> getAltIndexRowIds(
			final String altIdxTableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {
		return null;
	}

	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object, Object> callback,
			final DedupeFilter dedupeFilter,
			final String... authorizations ) {
		final CloseableIterator<CassandraRow> it = operations.getRows(
				index.getId().getString(),
				Lists.transform(
						dataIds,
						new Function<ByteArrayId, byte[]>() {
							@Override
							public byte[] apply(
									final ByteArrayId input ) {
								return input.getBytes();
							}
						}).toArray(
						new byte[][] {}),
				adapter.getAdapterId().getBytes(),
				authorizations);
		return new CloseableIteratorWrapper<>(
				it,
				new NativeEntryIteratorWrapper<Object>(
						this,
						adapterStore,
						index,
						it,
						null,
						(ScanCallback) callback,
						true));
	}

	@Override
	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final CassandraConstraintsQuery cassandraQuery = new CassandraConstraintsQuery(
				this,
				operations,
				adapterIdsToQuery,
				index,
				sanitizedQuery,
				filter,
				(ScanCallback<?, CassandraRow>) sanitizedQueryOptions.getScanCallback(),
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

		return cassandraQuery.query(
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
		final CassandraRowPrefixQuery<Object> prefixQuery = new CassandraRowPrefixQuery<Object>(
				this,
				operations,
				index,
				rowPrefix,
				(ScanCallback<Object, CassandraRow>) sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getLimit(),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());
		return prefixQuery.query(
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
		final CassandraRowIdsQuery<Object> q = new CassandraRowIdsQuery<Object>(
				this,
				operations,
				adapter,
				index,
				rowIds,
				(ScanCallback<Object, CassandraRow>) sanitizedQueryOptions.getScanCallback(),
				filter,
				sanitizedQueryOptions.getAuthorizations());

		return q.query(
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		// TODO Auto-generated method stub

	}

	@Override
	protected IndexWriter createIndexWriter(
			final DataAdapter adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable ) {
		return new CassandraIndexWriter(
				this,
				adapter,
				index,
				operations,
				callback,
				closable);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		// TODO Auto-generated method stub

	}

	@Override
	protected Iterable<GeoWaveRow> getRowsFromIngest(
			byte[] adapterId,
			DataStoreEntryInfo ingestInfo,
			List<FieldInfo<?>> fieldInfoList,
			boolean ensureUniqueId ) {
		final List<GeoWaveRow> rows = new ArrayList<GeoWaveRow>();

		// The single FieldInfo contains the fieldMask in the ID, and the
		// flattened fields in the written value
		byte[] fieldMask = fieldInfoList.get(
				0).getDataValue().getId().getBytes();
		byte[] value = fieldInfoList.get(
				0).getWrittenValue();

		Iterator<ByteArrayId> rowIdIterator = ingestInfo.getRowIds().iterator();

		for (final ByteArrayId insertionId : ingestInfo.getInsertionIds()) {
			byte[] uniqueDataId;
			if (ensureUniqueId) {
				uniqueDataId = DataStoreUtils.ensureUniqueId(
						ingestInfo.getDataId(),
						false).getBytes();
			}
			else {
				uniqueDataId = ingestInfo.getDataId();
			}

			// for each insertion(index) id, there's a matching rowId
			// that contains the duplicate count
			GeoWaveRow tempRow = new GeoWaveKeyImpl(
					rowIdIterator.next().getBytes());
			int numDuplicates = tempRow.getNumberOfDuplicates();

			rows.add(new CassandraRow(
					nextPartitionId(),
					uniqueDataId,
					adapterId,
					insertionId.getBytes(),
					fieldMask,
					value,
					numDuplicates));
		}

		return rows;
	}

	private byte[] nextPartitionId() {
		counter = (counter + 1) % PARTITIONS;

		return new byte[] {
			(byte) counter
		};
	}

	@Override
	public void write(
			Writer writer,
			Iterable<GeoWaveRow> rows,
			final String columnFamily ) {
		for (GeoWaveRow geowaveRow : rows) {
			CassandraRow cassRow = (CassandraRow) geowaveRow;
			((CassandraWriter) writer).write(cassRow);
		}
	}
}
