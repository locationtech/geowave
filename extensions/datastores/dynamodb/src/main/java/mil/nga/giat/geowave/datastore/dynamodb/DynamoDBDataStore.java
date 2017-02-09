package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

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
import mil.nga.giat.geowave.datastore.dynamodb.index.secondary.DynamoDBSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBAdapterStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBDataStatisticsStore;
import mil.nga.giat.geowave.datastore.dynamodb.metadata.DynamoDBIndexStore;
import mil.nga.giat.geowave.datastore.dynamodb.query.DynamoDBConstraintsQuery;
import mil.nga.giat.geowave.datastore.dynamodb.query.DynamoDBRowIdsQuery;
import mil.nga.giat.geowave.datastore.dynamodb.query.DynamoDBRowPrefixQuery;

public class DynamoDBDataStore extends
		BaseDataStore
{
	public final static String TYPE = "dynamodb";

	private final static Logger LOGGER = Logger.getLogger(DynamoDBDataStore.class);
	private final DynamoDBOperations dynamodbOperations;

	public DynamoDBDataStore(
			final DynamoDBOperations operations ) {
		super(
				new DynamoDBIndexStore(
						operations),
				new DynamoDBAdapterStore(
						operations),
				new DynamoDBDataStatisticsStore(
						operations),
				new DynamoDBAdapterIndexMappingStore(
						operations),
				new DynamoDBSecondaryIndexDataStore(
						operations),
				operations,
				operations.getOptions().getBaseOptions());
		dynamodbOperations = operations;
	}

	@Override
	protected IndexWriter createIndexWriter(
			final DataAdapter adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable ) {
		return new DynamoDBIndexWriter<>(
				adapter,
				index,
				dynamodbOperations,
				callback,
				closable);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		// TODO
	}

	@Override
	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final DynamoDBConstraintsQuery dynamodbQuery = new DynamoDBConstraintsQuery(
				dynamodbOperations,
				adapterIdsToQuery,
				index,
				sanitizedQuery,
				filter,
				(ScanCallback<Object, DynamoDBRow>) sanitizedQueryOptions.getScanCallback(),
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

		return dynamodbQuery.query(
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
		final DynamoDBRowPrefixQuery<Object> prefixQuery = new DynamoDBRowPrefixQuery<Object>(
				dynamodbOperations,
				index,
				rowPrefix,
				(ScanCallback<Object, DynamoDBRow>) sanitizedQueryOptions.getScanCallback(),
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
		final DynamoDBRowIdsQuery<Object> q = new DynamoDBRowIdsQuery<Object>(
				dynamodbOperations,
				adapter,
				index,
				rowIds,
				(ScanCallback<Object, DynamoDBRow>) sanitizedQueryOptions.getScanCallback(),
				filter,
				sanitizedQueryOptions.getAuthorizations());

		return q.query(
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object, Object> scanCallback,
			final DedupeFilter dedupeFilter,
			final String... authorizations ) {
		final Iterator<DynamoDBRow> it = dynamodbOperations.getRows(
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
		return new CloseableIterator.Wrapper<>(
				new NativeEntryIteratorWrapper<Object>(
						adapterStore,
						index,
						it,
						null,
						(ScanCallback) scanCallback,
						true));
	}

	@Override
	protected List<ByteArrayId> getAltIndexRowIds(
			final String tableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {
		// TODO

		return Collections.EMPTY_LIST;
	}

	@Override
	protected boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		return false;
	}

	@Override
	protected Deleter createIndexDeleter(
			final String indexTableName,
			final boolean isAltIndex,
			final String... authorizations )
			throws Exception {
		return new DynamoDBRowDeleter(
				dynamodbOperations,
				indexTableName,
				authorizations);
	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		// TODO Auto-generated method stub

	}
}