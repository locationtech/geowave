package mil.nga.giat.geowave.datastore.cassandra;

import java.io.Closeable;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.Deleter;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
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
	private final CassandraOperations operations;

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
				null,
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
			final ScanCallback<Object> callback,
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
						adapterStore,
						index,
						it,
						null,
						callback,
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
				operations,
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
				operations,
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
				operations,
				adapter,
				index,
				rowIds,
				(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
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

}
